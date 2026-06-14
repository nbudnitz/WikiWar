from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from functools import lru_cache
import math
import re
from typing import Any

import httpx

from .config import settings
from .segments import (
    api_url_for_wiki,
    clean_comment,
    clean_wikitext,
    fetch_revision_segments,
    first_sentence,
    is_graffiti_like_segment_text,
    normalize_candidate,
    period_bounds,
    top_contested_text_segments,
)


STRONG_DEBATE_RE = re.compile(
    r"\b(consensus|discuss(?:ion)?|debate|dispute|edit war|npov|pov|neutral|undue|"
    r"rfc|requested comment|wording|bias|controvers)\b",
    re.IGNORECASE,
)
SOURCE_DEBATE_RE = re.compile(
    r"\b(reliable source|source|citation|lead)\b",
    re.IGNORECASE,
)
MAX_RERANK_CANDIDATES = 40
RERANK_WORKERS = 6
RERANK_BATCH_SIZE = 12
TALK_REVISION_LIMIT = 35


def rerank_historical_rows(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    max_candidates: int = MAX_RERANK_CANDIDATES,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    enriched: list[dict[str, Any]] = []
    candidates = rows[: min(len(rows), max_candidates)]
    for start in range(0, len(candidates), RERANK_BATCH_SIZE):
        batch = candidates[start : start + RERANK_BATCH_SIZE]
        enriched.extend(enrich_historical_rows(batch))
        if len(positive_controversy_rows(enriched)) >= limit:
            break

    ranked = positive_controversy_rows(enriched)
    ranked.sort(
        key=lambda row: (
            float(row.get("controversy_score") or 0.0),
            int(row.get("battle_count") or 0),
            int(row.get("talk_evidence_count") or 0),
            float(row.get("peak_score") or 0.0),
        ),
        reverse=True,
    )
    for index, row in enumerate(ranked[:limit], start=1):
        row["rank"] = index
    return ranked[:limit]


def enrich_historical_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    enriched: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(RERANK_WORKERS, len(rows))) as executor:
        futures = {
            executor.submit(
                controversy_enrichment_for_page,
                str(row.get("wiki") or settings.wiki_db),
                int(row["page_id"]),
                str(row.get("page_title") or ""),
                str(row.get("period") or ""),
                float(row.get("peak_score") or 0.0),
                int(row.get("revert_count") or 0),
                int(row.get("mutual_revert_pairs") or 0),
            ): row
            for row in rows
        }
        for future in as_completed(futures):
            row = dict(futures[future])
            try:
                evidence = future.result()
            except Exception as exc:  # pragma: no cover - API failures should degrade.
                evidence = unavailable_evidence(str(exc))
            row.update(scoreboard_fields_from_evidence(evidence))
            row["controversy"] = evidence
            enriched.append(row)
    return enriched


def positive_controversy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if float(row.get("controversy_score") or 0.0) > 0.0]


@lru_cache(maxsize=512)
def controversy_enrichment_for_page(
    wiki: str,
    page_id: int,
    page_title: str,
    period: str,
    peak_score: float,
    revert_count: int,
    mutual_revert_pairs: int,
) -> dict[str, Any]:
    segments_payload = fetch_revision_segments(
        wiki=wiki,
        page_id=page_id,
        page_title=page_title,
        period=period,
    )
    segments = usable_battles(segments_payload.get("segments") or [])
    terms = controversy_terms(segments)
    talk = fetch_talk_page_evidence(
        wiki=wiki,
        page_title=page_title,
        period=period,
        terms=tuple(terms),
    )
    score = score_controversy_evidence(
        peak_score=peak_score,
        revert_count=revert_count,
        mutual_revert_pairs=mutual_revert_pairs,
        segments=segments,
        talk=talk,
    )
    return {
        "score": score["score"],
        "battle_score": score["battle_score"],
        "talk_score": score["talk_score"],
        "metadata_score": score["metadata_score"],
        "cleanup_penalty": score["cleanup_penalty"],
        "battle_count": len(segments),
        "talk_evidence_count": len(talk["evidence"]),
        "talk_evidence": talk["evidence"],
        "segments_source": segments_payload.get("source"),
        "revision_count": segments_payload.get("revision_count", 0),
        "revision_total": segments_payload.get("revision_total"),
        "status": "ok",
    }


def enrich_segments_payload(
    payload: dict[str, Any],
    *,
    wiki: str,
    page_title: str,
    period: str | None,
    peak_score: float = 0.0,
    revert_count: int = 0,
    mutual_revert_pairs: int = 0,
) -> dict[str, Any]:
    segments = usable_battles(payload.get("segments") or [])
    terms = controversy_terms(segments)
    talk = fetch_talk_page_evidence(
        wiki=wiki,
        page_title=page_title,
        period=period or "",
        terms=tuple(terms),
    )
    score = score_controversy_evidence(
        peak_score=peak_score,
        revert_count=revert_count,
        mutual_revert_pairs=mutual_revert_pairs,
        segments=segments,
        talk=talk,
    )
    return {
        **payload,
        "segments": segments,
        "controversy": {
            "score": score["score"],
            "battle_score": score["battle_score"],
            "talk_score": score["talk_score"],
            "metadata_score": score["metadata_score"],
            "cleanup_penalty": score["cleanup_penalty"],
            "battle_count": len(segments),
            "talk_evidence_count": len(talk["evidence"]),
            "talk_evidence": talk["evidence"],
            "status": "ok",
        },
    }


def build_local_evidence_payload(
    *,
    article_revisions: list[dict[str, Any]],
    talk_revisions: list[dict[str, Any]],
    wiki: str,
    page_id: int,
    page_title: str,
    period: str,
    peak_score: float = 0.0,
    revert_count: int = 0,
    mutual_revert_pairs: int = 0,
    source: str = "local_revision_dump",
) -> dict[str, Any]:
    article_revisions = sorted(
        article_revisions,
        key=lambda revision: (str(revision.get("timestamp") or ""), int(revision.get("rev_id") or 0)),
    )
    segment_rows = top_contested_text_segments(article_revisions)
    segments = usable_battles(segment_rows)
    terms = controversy_terms(segments)
    talk = talk_page_evidence_from_revisions(talk_revisions, terms=tuple(terms))
    score = score_controversy_evidence(
        peak_score=peak_score,
        revert_count=revert_count,
        mutual_revert_pairs=mutual_revert_pairs,
        segments=segments,
        talk=talk,
    )
    return {
        "source": source,
        "wiki": wiki,
        "page_id": page_id,
        "page_title": page_title,
        "period": period,
        "revision_count": len(article_revisions),
        "segments": segments,
        "controversy": {
            "score": score["score"],
            "battle_score": score["battle_score"],
            "talk_score": score["talk_score"],
            "metadata_score": score["metadata_score"],
            "cleanup_penalty": score["cleanup_penalty"],
            "battle_count": len(segments),
            "talk_evidence_count": len(talk["evidence"]),
            "talk_evidence": talk["evidence"],
            "status": "ok",
        },
    }


def scoreboard_fields_from_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "controversy_score": round(float(evidence.get("score") or 0.0), 2),
        "battle_score": round(float(evidence.get("battle_score") or 0.0), 2),
        "talk_score": round(float(evidence.get("talk_score") or 0.0), 2),
        "metadata_score": round(float(evidence.get("metadata_score") or 0.0), 2),
        "cleanup_penalty": round(float(evidence.get("cleanup_penalty") or 0.0), 2),
        "battle_count": int(evidence.get("battle_count") or 0),
        "talk_evidence_count": int(evidence.get("talk_evidence_count") or 0),
    }


def unavailable_evidence(message: str) -> dict[str, Any]:
    return {
        "score": 0.0,
        "battle_score": 0.0,
        "talk_score": 0.0,
        "metadata_score": 0.0,
        "cleanup_penalty": 0.0,
        "battle_count": 0,
        "talk_evidence_count": 0,
        "talk_evidence": [],
        "status": "unavailable",
        "error": message[:240],
    }


def usable_battles(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for segment in segments:
        if is_graffiti_like_segment_text(str(segment.get("changed_text") or segment.get("segment") or "")):
            continue
        combatants = int(segment.get("combatants") or segment.get("editors") or 0)
        reverts = int(segment.get("reverts") or 0)
        changes = int(segment.get("changes") or segment.get("edits") or 0)
        if combatants < 2:
            continue
        if reverts < 3 and changes < 3:
            continue
        result.append(segment)
    return result


def score_controversy_evidence(
    *,
    peak_score: float,
    revert_count: int,
    mutual_revert_pairs: int,
    segments: list[dict[str, Any]],
    talk: dict[str, Any],
) -> dict[str, float]:
    if not segments:
        return {
            "score": 0.0,
            "battle_score": 0.0,
            "talk_score": float(talk.get("score") or 0.0),
            "metadata_score": 0.0,
            "cleanup_penalty": 0.0,
        }

    battle_score = weighted_battle_score(segments)
    talk_score = float(talk.get("score") or 0.0)
    metadata_score = min(35.0, math.sqrt(max(0.0, peak_score)) * 1.35)
    metadata_score += min(15.0, math.sqrt(max(0, revert_count)) * 0.75)
    metadata_score += min(10.0, max(0, mutual_revert_pairs - 1) * 4.0)

    cleanup_share = cleanup_battle_share(segments)
    cleanup_penalty = 0.0
    if cleanup_share >= 0.7 and talk_score < 20:
        cleanup_penalty = battle_score * 0.9
    elif cleanup_share >= 0.45 and talk_score < 35:
        cleanup_penalty = battle_score * 0.5

    talk_multiplier = 1.0 + min(0.75, talk_score / 160.0)
    score = ((battle_score - cleanup_penalty) * talk_multiplier) + (talk_score * 2.2) + metadata_score
    if cleanup_share >= 0.7 and talk_score < 20:
        score = min(score, 25.0)
    if len(segments) == 1 and talk_score < 20:
        score = min(score, max(0.0, (battle_score * 0.18) + metadata_score))
    return {
        "score": round(max(0.0, score), 2),
        "battle_score": round(battle_score, 2),
        "talk_score": round(talk_score, 2),
        "metadata_score": round(metadata_score, 2),
        "cleanup_penalty": round(cleanup_penalty, 2),
    }


def weighted_battle_score(segments: list[dict[str, Any]]) -> float:
    weights = [1.0, 0.55, 0.25]
    total = 0.0
    for index, segment in enumerate(segments[:3]):
        score = float(segment.get("score") or 0.0)
        if str(segment.get("change_type") or "") == "swap":
            score *= 1.18
        total += score * weights[index]
    return total


def cleanup_battle_share(segments: list[dict[str, Any]]) -> float:
    total_comments = sum(int(segment.get("comment_count") or 0) for segment in segments)
    if not total_comments:
        return 0.0
    cleanup_comments = sum(int(segment.get("cleanup_comment_count") or 0) for segment in segments)
    return cleanup_comments / total_comments


def controversy_terms(segments: list[dict[str, Any]]) -> list[str]:
    terms: list[str] = []
    for segment in segments[:5]:
        terms.extend(str(value or "") for value in segment.get("swap_values") or [])
        terms.append(str(segment.get("changed_text") or ""))
        terms.append(str(segment.get("segment") or ""))
    seen: set[str] = set()
    result = []
    for term in terms:
        normalized = normalize_candidate(term)
        if len(normalized) < 4 or normalized in seen:
            continue
        if len(normalized.split()) == 1 and normalized in {"this", "that", "with", "from", "have"}:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result[:18]


@lru_cache(maxsize=512)
def fetch_talk_page_evidence(
    *,
    wiki: str,
    page_title: str,
    period: str,
    terms: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not page_title:
        return {"score": 0.0, "evidence": []}
    start, end = period_bounds(period)
    if start is None or end is None:
        return {"score": 0.0, "evidence": []}

    params: dict[str, Any] = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "revisions",
        "titles": f"Talk:{page_title.replace('_', ' ')}",
        "rvprop": "ids|timestamp|user|comment|content",
        "rvslots": "main",
        "rvdir": "newer",
        "rvlimit": TALK_REVISION_LIMIT,
        "rvstart": start.isoformat().replace("+00:00", "Z"),
        "rvend": end.isoformat().replace("+00:00", "Z"),
    }
    evidence: list[dict[str, Any]] = []
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=20.0) as client:
        response = client.get(api_url_for_wiki(wiki), params=params)
        response.raise_for_status()
        pages = response.json().get("query", {}).get("pages", [])
    revisions = pages[0].get("revisions", []) if pages and not pages[0].get("missing") else []
    seen_sentences: set[str] = set()
    previous_content = ""
    for revision in revisions:
        comment = clean_comment(str(revision.get("comment") or ""))
        content = revision_content_text(revision)
        sentences = talk_added_sentences(previous_content, content)
        if not sentences and comment:
            sentences = [first_sentence(comment)]
        previous_content = content

        for sentence in sentences:
            if not sentence:
                continue
            haystack = normalize_candidate(" ".join([comment, sentence]))
            term_matches = [term for term in terms if term and term in haystack]
            dispute = bool(STRONG_DEBATE_RE.search(haystack)) or (
                bool(SOURCE_DEBATE_RE.search(haystack)) and bool(term_matches)
            )
            if not dispute:
                continue
            sentence_key = normalize_candidate(sentence or comment)
            if sentence_key in seen_sentences:
                continue
            seen_sentences.add(sentence_key)
            evidence.append(
                {
                    "timestamp": revision.get("timestamp") or "",
                    "user": revision.get("user") or "",
                    "comment": comment,
                    "sentence": sentence,
                    "matched_terms": term_matches[:5],
                    "dispute": dispute,
                }
            )
            break
        if len(evidence) >= 5:
            break
    if not evidence and previous_content:
        evidence.extend(snapshot_talk_evidence(previous_content, terms))
    return {"score": talk_score(evidence), "evidence": evidence}


def talk_page_evidence_from_revisions(
    revisions: list[dict[str, Any]],
    *,
    terms: tuple[str, ...] = (),
) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    seen_sentences: set[str] = set()
    previous_content = ""
    for revision in sorted(revisions, key=lambda item: (str(item.get("timestamp") or ""), int(item.get("rev_id") or 0))):
        comment = clean_comment(str(revision.get("comment") or ""))
        content = clean_wikitext(str(revision.get("content") or ""))
        sentences = talk_added_sentences(previous_content, content)
        if not sentences and comment:
            sentences = [first_sentence(comment)]
        previous_content = content

        for sentence in sentences:
            if not sentence:
                continue
            haystack = normalize_candidate(" ".join([comment, sentence]))
            term_matches = [term for term in terms if term and term in haystack]
            dispute = bool(STRONG_DEBATE_RE.search(haystack)) or (
                bool(SOURCE_DEBATE_RE.search(haystack)) and bool(term_matches)
            )
            if not dispute:
                continue
            sentence_key = normalize_candidate(sentence or comment)
            if sentence_key in seen_sentences:
                continue
            seen_sentences.add(sentence_key)
            evidence.append(
                {
                    "timestamp": revision.get("timestamp") or "",
                    "user": revision.get("user_text") or revision.get("user") or "",
                    "comment": comment,
                    "sentence": sentence,
                    "matched_terms": term_matches[:5],
                    "dispute": dispute,
                }
            )
            break
        if len(evidence) >= 5:
            break
    if not evidence and previous_content:
        evidence.extend(snapshot_talk_evidence(previous_content, terms))
    return {"score": talk_score(evidence), "evidence": evidence}


def revision_content_text(revision: dict[str, Any]) -> str:
    slots = revision.get("slots") or {}
    main = slots.get("main") or {}
    return clean_wikitext(str(main.get("content") or revision.get("content") or revision.get("*") or ""))


def talk_sentence(content: str, terms: tuple[str, ...]) -> str:
    if not content:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", content)
    for sentence in sentences:
        normalized = normalize_candidate(sentence)
        if 20 <= len(sentence) <= 260 and (
            STRONG_DEBATE_RE.search(sentence)
            or (SOURCE_DEBATE_RE.search(sentence) and any(term in normalized for term in terms))
            or any(term in normalized for term in terms)
        ):
            return sentence.strip()
    return ""


def talk_added_sentences(previous: str, current: str) -> list[str]:
    if not current:
        return []
    previous_sentences = split_talk_sentences(previous)
    current_sentences = split_talk_sentences(current)
    if not previous_sentences:
        return current_sentences[:6]
    matcher = SequenceMatcher(
        None,
        [normalize_candidate(sentence) for sentence in previous_sentences],
        [normalize_candidate(sentence) for sentence in current_sentences],
        autojunk=False,
    )
    added: list[str] = []
    for tag, _, _, right_start, right_end in matcher.get_opcodes():
        if tag == "equal":
            continue
        for sentence in current_sentences[right_start:right_end]:
            if is_talk_sentence_candidate(sentence):
                added.append(sentence)
                if len(added) >= 6:
                    return added
    return added


def snapshot_talk_evidence(content: str, terms: tuple[str, ...]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sentence in split_talk_sentences(content):
        haystack = normalize_candidate(sentence)
        term_matches = [term for term in terms if term and term in haystack]
        dispute = bool(STRONG_DEBATE_RE.search(haystack)) or (
            bool(SOURCE_DEBATE_RE.search(haystack)) and bool(term_matches)
        )
        if not dispute:
            continue
        sentence_key = normalize_candidate(sentence)
        if sentence_key in seen:
            continue
        seen.add(sentence_key)
        evidence.append(
            {
                "timestamp": "",
                "user": "",
                "comment": "",
                "sentence": sentence,
                "matched_terms": term_matches[:5],
                "dispute": True,
            }
        )
        if len(evidence) >= 3:
            break
    return evidence


def split_talk_sentences(content: str) -> list[str]:
    return [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+", content)
        if is_talk_sentence_candidate(sentence)
    ]


def is_talk_sentence_candidate(sentence: str) -> bool:
    if not 20 <= len(sentence) <= 280:
        return False
    normalized = normalize_candidate(sentence)
    if normalized.startswith("archives talk"):
        return False
    if normalized.count("archive") >= 3:
        return False
    return True


def talk_score(evidence: list[dict[str, Any]]) -> float:
    if not evidence:
        return 0.0
    users = {str(item.get("user") or "") for item in evidence if item.get("user")}
    total = min(35.0, len(users) * 7.0)
    for item in evidence:
        item_score = 10.0
        if item.get("dispute"):
            item_score += 14.0
        if item.get("matched_terms"):
            item_score += 18.0
        total += item_score
    return round(min(160.0, total), 2)
