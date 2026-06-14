from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
import html
from functools import lru_cache
import re
from typing import Any

import httpx

from .config import settings


REVERT_RE = re.compile(r"\b(revert(?:ed|ing)?|rvv?|rollback|undid|undo)\b", re.IGNORECASE)
CLEANUP_RE = re.compile(
    r"\b(vandal(?:ism|s)?|rvv|graffiti|blank(?:ing|ed)?|test edit|nonsense|spam|hoax|unconstructive)\b",
    re.IGNORECASE,
)
GENERATED_REVISION_COMMENT_RE = re.compile(
    r"(?:^|\b)(?:undid|undo|reverted|revert|rollback)\b[^\n]{0,140}\brevision\s+\d+\b"
    r"|^\s*revision\s+\d+\s+by\s*(?:\([^)]*\))?\s*$"
    r"|^\s*reverted\s+(?:\d+\s+)?edits?\s+by\b",
    re.IGNORECASE,
)
GENERIC_COMMENT_ACTION_RE = re.compile(
    r"^(?:rvv?|revert(?:ed|ing)?|undo|undid|rollback|restore(?:d)?|copyedit|ce|cleanup|fix(?:ed)?)\b[:\s-]*",
    re.IGNORECASE,
)
WORD_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9'’\-–]*")
WIKILINK_RE = re.compile(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]")
TEMPLATE_RE = re.compile(r"\{\{[^{}]*\}\}")
REF_RE = re.compile(r"<ref\b[^>]*>.*?</ref>|<ref\b[^/]*/>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
SECTION_RE = re.compile(r"={2,}\s*([^=]+?)\s*={2,}")
STOPWORDS = {
    "about",
    "after",
    "also",
    "because",
    "been",
    "being",
    "from",
    "have",
    "into",
    "that",
    "their",
    "there",
    "these",
    "this",
    "those",
    "were",
    "with",
    "would",
}


FULL_CONTENT_REVISION_LIMIT = 220
REVISION_CONTENT_BATCH_SIZE = 50
MIN_TOP_BATTLE_SIGNAL = 3


@dataclass(frozen=True)
class WordToken:
    text: str
    start: int
    end: int


@lru_cache(maxsize=128)
def fetch_revision_segments(
    *,
    wiki: str,
    page_id: int,
    page_title: str | None = None,
    period: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    revisions = fetch_revisions_metadata(
        wiki=wiki,
        page_id=page_id,
        page_title=page_title,
        period=period,
        limit=limit,
    )
    if len(revisions) > FULL_CONTENT_REVISION_LIMIT:
        revision_pairs, content_revision_count = fetch_revert_content_pairs(wiki, revisions)
        return {
            "source": "mediawiki_api_revert_content_diff",
            "revision_count": content_revision_count,
            "revision_total": len(revisions),
            "segments": top_contested_revision_pairs(revision_pairs),
        }

    revisions = hydrate_revision_content(wiki, revisions)
    return {
        "source": "mediawiki_api_content_diff",
        "revision_count": len(revisions),
        "segments": top_contested_text_segments(revisions),
    }


def fetch_revisions_with_content(
    *,
    wiki: str,
    page_id: int,
    page_title: str | None = None,
    period: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    start, end = period_bounds(period)
    has_range = start is not None and end is not None
    base_params: dict[str, Any] = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "revisions",
        "rvprop": "ids|timestamp|user|comment|tags|content",
        "rvslots": "main",
        "rvdir": "newer" if has_range else "older",
    }
    if page_id:
        base_params["pageids"] = page_id
    elif page_title:
        base_params["titles"] = page_title
    if start and end:
        base_params["rvstart"] = start.isoformat().replace("+00:00", "Z")
        base_params["rvend"] = end.isoformat().replace("+00:00", "Z")

    raw_revisions: list[dict[str, Any]] = []
    continuation: dict[str, Any] = {}
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0) as client:
        while len(raw_revisions) < limit:
            params = {
                **base_params,
                **continuation,
                "rvlimit": min(50, limit - len(raw_revisions)),
            }
            response = client.get(api_url_for_wiki(wiki), params=params)
            response.raise_for_status()
            payload = response.json()
            pages = payload.get("query", {}).get("pages", [])
            raw_revisions.extend(pages[0].get("revisions", []) if pages else [])
            continuation = payload.get("continue") or {}
            if not continuation:
                break

    revisions = [
        {
            "rev_id": revision.get("revid"),
            "timestamp": revision.get("timestamp") or "",
            "user_text": revision.get("user") or "",
            "comment": revision.get("comment") or "",
            "tags": revision.get("tags") or [],
            "content": revision_content(revision),
        }
        for revision in raw_revisions
        if revision_content(revision)
    ]
    return sorted(revisions, key=lambda item: (item["timestamp"], item.get("rev_id") or 0))


def fetch_revisions_metadata(
    *,
    wiki: str,
    page_id: int,
    page_title: str | None = None,
    period: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    start, end = period_bounds(period)
    has_range = start is not None and end is not None
    base_params: dict[str, Any] = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "prop": "revisions",
        "rvprop": "ids|timestamp|user|comment|tags",
        "rvdir": "newer" if has_range else "older",
    }
    if page_id:
        base_params["pageids"] = page_id
    elif page_title:
        base_params["titles"] = page_title
    if start and end:
        base_params["rvstart"] = start.isoformat().replace("+00:00", "Z")
        base_params["rvend"] = end.isoformat().replace("+00:00", "Z")

    raw_revisions: list[dict[str, Any]] = []
    continuation: dict[str, Any] = {}
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0) as client:
        while len(raw_revisions) < limit:
            params = {
                **base_params,
                **continuation,
                "rvlimit": min(50, limit - len(raw_revisions)),
            }
            response = client.get(api_url_for_wiki(wiki), params=params)
            response.raise_for_status()
            payload = response.json()
            pages = payload.get("query", {}).get("pages", [])
            raw_revisions.extend(pages[0].get("revisions", []) if pages else [])
            continuation = payload.get("continue") or {}
            if not continuation:
                break

    revisions = [
        {
            "rev_id": revision.get("revid"),
            "timestamp": revision.get("timestamp") or "",
            "user_text": revision.get("user") or "",
            "comment": revision.get("comment") or "",
            "tags": revision.get("tags") or [],
        }
        for revision in raw_revisions
        if revision.get("revid") is not None
    ]
    return sorted(revisions, key=lambda item: (item["timestamp"], item.get("rev_id") or 0))


def hydrate_revision_content(wiki: str, revisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    content_by_id = fetch_revision_content_by_ids(
        wiki,
        [int(revision["rev_id"]) for revision in revisions if revision.get("rev_id") is not None],
    )
    hydrated = []
    for revision in revisions:
        rev_id = revision.get("rev_id")
        content_revision = content_by_id.get(int(rev_id)) if rev_id is not None else None
        if not content_revision or not content_revision.get("content"):
            continue
        hydrated.append({**revision, "content": content_revision["content"]})
    return hydrated


def fetch_revert_content_pairs(
    wiki: str,
    revisions: list[dict[str, Any]],
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], int]:
    metadata_pairs = [
        (previous, current)
        for previous, current in zip(revisions, revisions[1:])
        if is_revert_like(current)
    ]
    target_ids = sorted(
        {
            int(revision["rev_id"])
            for pair in metadata_pairs
            for revision in pair
            if revision.get("rev_id") is not None
        }
    )
    content_by_id = fetch_revision_content_by_ids(wiki, target_ids)
    revision_pairs = []
    for previous, current in metadata_pairs:
        previous_content = content_by_id.get(int(previous["rev_id"]))
        current_content = content_by_id.get(int(current["rev_id"]))
        if not previous_content or not current_content:
            continue
        if not previous_content.get("content") or not current_content.get("content"):
            continue
        revision_pairs.append(
            (
                {**previous, "content": previous_content["content"]},
                {**current, "content": current_content["content"]},
            )
        )
    return revision_pairs, len(content_by_id)


def fetch_revision_content_by_ids(wiki: str, revision_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not revision_ids:
        return {}
    content_by_id: dict[int, dict[str, Any]] = {}
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0) as client:
        for start in range(0, len(revision_ids), REVISION_CONTENT_BATCH_SIZE):
            batch = revision_ids[start : start + REVISION_CONTENT_BATCH_SIZE]
            response = client.get(
                api_url_for_wiki(wiki),
                params={
                    "action": "query",
                    "format": "json",
                    "formatversion": "2",
                    "prop": "revisions",
                    "revids": "|".join(str(rev_id) for rev_id in batch),
                    "rvprop": "ids|timestamp|user|comment|tags|content",
                    "rvslots": "main",
                },
            )
            response.raise_for_status()
            payload = response.json()
            for page in payload.get("query", {}).get("pages", []):
                for revision in page.get("revisions", []):
                    rev_id = revision.get("revid")
                    if rev_id is None:
                        continue
                    content_by_id[int(rev_id)] = {
                        "rev_id": rev_id,
                        "timestamp": revision.get("timestamp") or "",
                        "user_text": revision.get("user") or "",
                        "comment": revision.get("comment") or "",
                        "tags": revision.get("tags") or [],
                        "content": revision_content(revision),
                    }
    return content_by_id


def top_contested_text_segments(
    revisions: list[dict[str, Any]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    return top_contested_revision_pairs(list(zip(revisions, revisions[1:])), limit=limit)


def top_contested_revision_pairs(
    revision_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "segment": "",
            "changed_text": "",
            "highlight_before": "",
            "highlight_after": "",
            "context_before": "",
            "context_after": "",
            "change_types": Counter(),
            "swap_values": Counter(),
            "changes": 0,
            "reverts": 0,
            "changed_word_count": 0,
            "first_shot_comment": "",
            "cleanup_comment_count": 0,
            "comment_count": 0,
            "revision_ids": set(),
            "revert_revision_ids": set(),
            "editors": set(),
            "examples": [],
        }
    )
    for previous, current in revision_pairs:
        previous_user = str(previous.get("user_text") or "")
        previous_comment = str(previous.get("comment") or "")
        current_user = str(current.get("user_text") or "")
        current_is_revert = is_revert_like(current)
        current_comment = str(current.get("comment") or "")
        current_rev_id = current.get("rev_id")
        snippets = changed_text_segments(str(previous.get("content") or ""), str(current.get("content") or ""))
        for snippet in snippets:
            key = str(snippet.get("group_key") or normalize_candidate(snippet["segment"]))
            item = stats[key]
            item["segment"] = item["segment"] or snippet["segment"]
            item["changed_text"] = item["changed_text"] or snippet["changed_text"]
            item["highlight_before"] = item["highlight_before"] or snippet["highlight_before"]
            item["highlight_after"] = item["highlight_after"] or snippet["highlight_after"]
            item["context_before"] = item["context_before"] or snippet["context_before"]
            item["context_after"] = item["context_after"] or snippet["context_after"]
            item["change_types"][snippet.get("change_type", "change")] += 1
            item["changed_word_count"] = max(
                int(item["changed_word_count"] or 0),
                int(snippet.get("changed_word_count") or 0),
            )
            for value in snippet.get("swap_values", []):
                item["swap_values"][value] += 1
            item["changes"] += 1
            first_shot = substantive_first_shot_comment(previous_comment if current_is_revert else current_comment)
            if first_shot and not item["first_shot_comment"]:
                item["first_shot_comment"] = first_shot
            for comment in (previous_comment, current_comment):
                if comment:
                    item["comment_count"] += 1
                    if is_cleanup_like_comment(comment):
                        item["cleanup_comment_count"] += 1
            if current_rev_id is not None:
                item["revision_ids"].add(current_rev_id)
            if current_user:
                item["editors"].add(current_user)
            if current_is_revert and previous_user and previous_user != current_user:
                item["editors"].add(previous_user)
            if current_is_revert:
                item["reverts"] += 1
                if current_rev_id is not None:
                    item["revert_revision_ids"].add(current_rev_id)
            if current_comment and len(item["examples"]) < 2:
                item["examples"].append(current_comment)

    rows = []
    for values in stats.values():
        changes = int(values["changes"])
        reverts = int(values["reverts"])
        if not top_battle_signal_enough(changes, reverts):
            continue
        editors = len(values["editors"])
        change_type = values["change_types"].most_common(1)[0][0] if values["change_types"] else "change"
        changed_word_count = int(values["changed_word_count"] or 0)
        cleanup_comment_count = int(values["cleanup_comment_count"] or 0)
        comment_count = int(values["comment_count"] or 0)
        score = segment_battle_score(
            changes=changes,
            reverts=reverts,
            editors=editors,
            changed_word_count=changed_word_count,
            change_type=change_type,
        )
        if score <= 0:
            continue
        rows.append(
            {
                "segment": values["segment"],
                "changed_text": values["changed_text"],
                "highlight_before": values["highlight_before"],
                "highlight_after": values["highlight_after"],
                "context_before": values["context_before"],
                "context_after": values["context_after"],
                "change_type": change_type,
                "swap_values": [value for value, _ in values["swap_values"].most_common()],
                "changed_word_count": changed_word_count,
                "first_shot_comment": values["first_shot_comment"],
                "cleanup_comment_count": cleanup_comment_count,
                "comment_count": comment_count,
                "cleanup_comment_share": round(cleanup_comment_count / comment_count, 3) if comment_count else 0.0,
                "score": score,
                "changes": changes,
                "edits": changes,
                "reverts": reverts,
                "editors": editors,
                "combatants": editors,
                "examples": values["examples"],
                "_revision_ids": values["revision_ids"],
                "_revert_revision_ids": values["revert_revision_ids"],
                "_editor_names": values["editors"],
            }
        )

    rows.sort(key=lambda row: (row["score"], row["reverts"], row["changes"], row["editors"]), reverse=True)
    return grouped_diverse_segments(rows, limit)


def top_battle_signal_enough(changes: int, reverts: int) -> bool:
    return changes >= MIN_TOP_BATTLE_SIGNAL or reverts >= MIN_TOP_BATTLE_SIGNAL


def first_sentence(value: str) -> str:
    cleaned = clean_comment(value)
    if not cleaned:
        return ""
    match = re.search(r"(.{12,240}?[.!?])(?:\s|$)", cleaned)
    if match:
        return match.group(1).strip()
    return cleaned[:240].strip()


def substantive_first_shot_comment(value: str) -> str:
    if not value:
        return ""
    cleaned = first_sentence(value)
    if not cleaned:
        return ""
    if is_cleanup_like_comment(cleaned):
        return ""
    if is_generated_revision_comment(value) or is_generated_revision_comment(cleaned):
        return ""
    if not has_substantive_comment_text(cleaned):
        return ""
    return cleaned


def is_generated_revision_comment(value: str) -> bool:
    raw = html.unescape(value or "")
    raw = TAG_RE.sub(" ", raw)
    cleaned = clean_comment(value)
    return bool(
        GENERATED_REVISION_COMMENT_RE.search(raw)
        or GENERATED_REVISION_COMMENT_RE.search(cleaned)
    )


def has_substantive_comment_text(value: str) -> bool:
    stripped = GENERIC_COMMENT_ACTION_RE.sub("", value or "").strip()
    if not stripped:
        return False
    words = [
        word.lower()
        for word in WORD_RE.findall(stripped)
        if not word.isdigit()
    ]
    content_words = [word for word in words if word not in STOPWORDS]
    return len(content_words) >= 3


def clean_comment(value: str) -> str:
    value = html.unescape(value or "")
    value = TAG_RE.sub(" ", value)
    value = re.sub(r"/\*.*?\*/", " ", value)
    value = re.sub(r"\[\[[^\]]+\]\]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -:")


def is_cleanup_like_comment(value: str) -> bool:
    return bool(CLEANUP_RE.search(value or ""))


def segment_battle_score(
    *,
    changes: int,
    reverts: int,
    editors: int,
    changed_word_count: int,
    change_type: str,
) -> float:
    if editors < 2:
        return 0.0

    base = (reverts * 12.0) + (changes * 2.0) + min(10.0, max(0, editors - 1) * 3.0)
    if change_type == "swap" and changed_word_count <= 6:
        base += 8.0
    if reverts < 3:
        base *= 0.45
    return round(base * segment_size_multiplier(changed_word_count), 2)


def segment_size_multiplier(changed_word_count: int) -> float:
    if changed_word_count <= 0:
        return 1.0
    if changed_word_count <= 6:
        return 1.12
    if changed_word_count <= 14:
        return 1.0
    if changed_word_count <= 28:
        return 0.72
    if changed_word_count <= 50:
        return 0.45
    return 0.25


def changed_text_snippets(before: str, after: str, *, max_snippets: int = 20) -> list[str]:
    return [entry["segment"] for entry in changed_text_segments(before, after, max_snippets=max_snippets)]


def changed_text_segments(before: str, after: str, *, max_snippets: int = 20) -> list[dict[str, Any]]:
    before_text = clean_wikitext(before)
    after_text = clean_wikitext(after)
    before_tokens = word_tokens(before_text)
    after_tokens = word_tokens(after_text)
    if not before_tokens or not after_tokens:
        return []
    before_words = [token.text for token in before_tokens]
    after_words = [token.text for token in after_tokens]
    prefix, before_suffix, after_suffix = changed_token_bounds(before_words, after_words)
    if prefix == before_suffix and prefix == after_suffix:
        return []

    snippets: list[dict[str, Any]] = []
    seen: set[str] = set()
    matcher = SequenceMatcher(
        None,
        before_words[prefix:before_suffix],
        after_words[prefix:after_suffix],
        autojunk=False,
    )
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag == "equal":
            continue
        left_start += prefix
        left_end += prefix
        right_start += prefix
        right_end += prefix
        for candidate in opcode_segments(
            tag,
            before_tokens,
            after_tokens,
            before_text,
            after_text,
            left_start,
            left_end,
            right_start,
            right_end,
        ):
            normalized = str(candidate.get("group_key") or normalize_candidate(candidate["segment"]))
            if normalized and normalized not in seen:
                seen.add(normalized)
                snippets.append(candidate)
                if len(snippets) >= max_snippets:
                    return snippets
    return snippets


def changed_token_bounds(before_words: list[str], after_words: list[str]) -> tuple[int, int, int]:
    prefix = 0
    before_len = len(before_words)
    after_len = len(after_words)
    while prefix < before_len and prefix < after_len and before_words[prefix] == after_words[prefix]:
        prefix += 1

    before_suffix = before_len
    after_suffix = after_len
    while (
        before_suffix > prefix
        and after_suffix > prefix
        and before_words[before_suffix - 1] == after_words[after_suffix - 1]
    ):
        before_suffix -= 1
        after_suffix -= 1

    return prefix, before_suffix, after_suffix


def opcode_segments(
    tag: str,
    before_tokens: list[WordToken],
    after_tokens: list[WordToken],
    before_text: str,
    after_text: str,
    left_start: int,
    left_end: int,
    right_start: int,
    right_end: int,
) -> list[dict[str, Any]]:
    if tag == "replace":
        before_candidate = candidate_segment(before_tokens, before_text, left_start, left_end)
        after_candidate = candidate_segment(after_tokens, after_text, right_start, right_end)
        if before_candidate and after_candidate:
            candidate = swap_segment(before_candidate, after_candidate)
            return [candidate] if candidate else []
        if before_candidate:
            before_candidate["change_type"] = "deletion"
            return [before_candidate]
        if after_candidate:
            after_candidate["change_type"] = "addition"
            return [after_candidate]
        return []
    if tag == "delete":
        candidate = candidate_segment(before_tokens, before_text, left_start, left_end)
        if candidate:
            candidate["change_type"] = "deletion"
            return [candidate]
        return []
    if tag == "insert":
        candidate = candidate_segment(after_tokens, after_text, right_start, right_end)
        if candidate:
            candidate["change_type"] = "addition"
            return [candidate]
        return []
    return []


def candidate_segment(tokens: list[WordToken], source: str, start: int, end: int) -> dict[str, Any] | None:
    words = [token.text for token in tokens[start:end] if WORD_RE.fullmatch(token.text)]
    if not words:
        return None
    changed_text = token_fragment(source, tokens, start, end)
    if is_graffiti_like_segment_text(changed_text):
        return None
    changed_word_count = len(words)
    if len(words) == 1:
        word = words[0]
        if len(word) >= 4 and word.lower() not in STOPWORDS:
            segment_words = [word]
        else:
            return None
    elif len(words) <= 14:
        segment_words = words
    elif len(words) <= 35:
        segment_words = words
    else:
        segment_words = words[:10]

    changed_start = max(start, 0)
    changed_end = min(changed_start + len(segment_words), len(tokens))
    segment_start, segment_end = expanded_phrase_bounds(tokens, changed_start, changed_end)
    segment = token_fragment(source, tokens, segment_start, segment_end)
    return {
        "segment": segment,
        "changed_text": token_fragment(source, tokens, changed_start, changed_end),
        "highlight_before": token_fragment(source, tokens, segment_start, changed_start),
        "highlight_after": token_fragment(source, tokens, changed_end, segment_end),
        "context_before": token_fragment(source, tokens, max(0, segment_start - 6), segment_start),
        "context_after": token_fragment(source, tokens, segment_end, min(len(tokens), segment_end + 6)),
        "change_type": "change",
        "swap_values": [],
        "changed_word_count": changed_word_count,
    }


def swap_segment(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_text = str(before["changed_text"])
    after_text = str(after["changed_text"])
    if is_graffiti_like_segment_text(before_text) or is_graffiti_like_segment_text(after_text):
        return {}
    values = unique_phrases([before_text, after_text])
    result = dict(after)
    result["change_type"] = "swap"
    result["swapped_from"] = before_text
    result["swapped_to"] = after_text
    result["swap_values"] = values
    result["changed_word_count"] = max(
        int(before.get("changed_word_count") or 0),
        int(after.get("changed_word_count") or 0),
    )
    result["group_key"] = swap_group_key(before, after, values)
    return result


def swap_group_key(before: dict[str, Any], after: dict[str, Any], values: list[str]) -> str:
    context_before = normalize_candidate(str(after.get("context_before") or before.get("context_before") or ""))
    context_after = normalize_candidate(str(after.get("context_after") or before.get("context_after") or ""))
    value_key = "|".join(sorted(normalize_candidate(value) for value in values))
    return f"swap:{context_before}:{context_after}:{value_key}"


def unique_phrases(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = normalize_candidate(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(value)
    return result


def is_graffiti_like_segment_text(value: str) -> bool:
    compact = re.sub(r"[^A-Za-z0-9]+", "", value or "").lower()
    if len(compact) >= 40 and not any(char.isdigit() for char in compact) and repeated_unit_ratio(compact) >= 0.82:
        return True

    words = [word.lower() for word in WORD_RE.findall(value or "")]
    if len(words) >= 8:
        top_count = Counter(words).most_common(1)[0][1]
        if top_count / len(words) >= 0.7:
            return True

    long_words = [word.lower() for word in words if len(word) >= 40]
    return any(repeated_unit_ratio(word) >= 0.82 for word in long_words)


def repeated_unit_ratio(value: str) -> float:
    if not value:
        return 0.0
    best = 0.0
    max_unit = min(24, max(1, len(value) // 2))
    for size in range(1, max_unit + 1):
        unit = value[:size]
        if not unit:
            continue
        repeated = (unit * ((len(value) // size) + 1))[: len(value)]
        matches = sum(1 for left, right in zip(value, repeated) if left == right)
        best = max(best, matches / len(value))
    return best


def expanded_phrase_bounds(tokens: list[WordToken], start: int, end: int) -> tuple[int, int]:
    expanded_start = start
    expanded_end = end

    if end - start <= 3 and start >= 2 and tokens[start - 1].text.lower() == "of":
        anchor = start - 2
        if is_title_phrase_token(tokens[anchor].text):
            expanded_start = anchor
            while expanded_start > 0 and is_title_phrase_token(tokens[expanded_start - 1].text):
                expanded_start -= 1

    if end - start <= 3 and end + 1 < len(tokens) and tokens[end].text.lower() == "of":
        anchor = end + 1
        if is_title_phrase_token(tokens[anchor].text):
            expanded_end = anchor + 1
            while expanded_end < len(tokens) and is_title_phrase_token(tokens[expanded_end].text):
                expanded_end += 1

    return expanded_start, expanded_end


def is_title_phrase_token(token: str) -> bool:
    return bool(token) and token[0].isupper() and token.lower() not in STOPWORDS


def clean_wikitext(value: str) -> str:
    value = html.unescape(value)
    value = REF_RE.sub(" ", value)
    value = TEMPLATE_RE.sub(" ", value)
    value = WIKILINK_RE.sub(r"\1", value)
    value = SECTION_RE.sub(r" \1. ", value)
    value = TAG_RE.sub(" ", value)
    value = value.replace("'''", "").replace("''", "")
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"[\[\]{}|=*#_:;]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def word_tokens(value: str) -> list[WordToken]:
    return [
        WordToken(text=match.group(0), start=match.start(), end=match.end())
        for match in WORD_RE.finditer(value)
    ]


def text_tokens(value: str) -> list[str]:
    return [token.text for token in word_tokens(value)]


def token_fragment(source: str, tokens: list[WordToken], start: int, end: int) -> str:
    if start >= end or start < 0 or end > len(tokens):
        return ""
    return source[tokens[start].start : tokens[end - 1].end].strip()


def phrase(words: list[str]) -> str:
    return " ".join(words).strip()


def normalize_candidate(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower()).strip()


def diverse_segments(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not any(segments_overlap(str(row["segment"]), str(existing["segment"])) for existing in selected):
            selected.append(public_segment_row(row))
        if len(selected) == limit:
            return selected
    return selected


def grouped_diverse_segments(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    used: set[int] = set()
    for index, row in enumerate(rows):
        if index in used:
            continue
        if any(segments_overlap(str(row["segment"]), str(existing["segment"])) for existing in selected):
            used.add(index)
            continue

        group = [row]
        used.add(index)
        for candidate_index, candidate in enumerate(rows[index + 1 :], start=index + 1):
            if candidate_index in used:
                continue
            if any(segments_overlap(str(candidate["segment"]), str(existing["segment"])) for existing in selected):
                used.add(candidate_index)
                continue
            if comoving_segments(row, candidate):
                group.append(candidate)
                used.add(candidate_index)

        selected.append(combine_segment_group(group))
        if len(selected) == limit:
            return selected
    return selected


def comoving_segments(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_reverts = set(left.get("_revert_revision_ids") or set())
    right_reverts = set(right.get("_revert_revision_ids") or set())
    if len(left_reverts) < 4 or len(right_reverts) < 4:
        return False
    intersection = len(left_reverts & right_reverts)
    union = len(left_reverts | right_reverts)
    if not union:
        return False
    return intersection / union >= 0.85


def combine_segment_group(group: list[dict[str, Any]]) -> dict[str, Any]:
    if not group:
        return {}
    primary = dict(group[0])
    if len(group) == 1:
        return public_segment_row(primary)

    revision_ids = set().union(*(set(row.get("_revision_ids") or set()) for row in group))
    revert_revision_ids = set().union(*(set(row.get("_revert_revision_ids") or set()) for row in group))
    editor_names = set().union(*(set(row.get("_editor_names") or set()) for row in group))
    changes = len(revision_ids) or max(int(row.get("changes") or 0) for row in group)
    reverts = len(revert_revision_ids) or max(int(row.get("reverts") or 0) for row in group)
    editors = len([editor for editor in editor_names if editor])
    changed_word_count = max(int(row.get("changed_word_count") or 0) for row in group)
    change_type = str(primary.get("change_type") or "change")
    primary["changes"] = changes
    primary["edits"] = changes
    primary["reverts"] = reverts
    primary["editors"] = editors
    primary["combatants"] = editors
    primary["changed_word_count"] = changed_word_count
    primary["cleanup_comment_count"] = sum(int(row.get("cleanup_comment_count") or 0) for row in group)
    primary["comment_count"] = sum(int(row.get("comment_count") or 0) for row in group)
    primary["cleanup_comment_share"] = (
        round(primary["cleanup_comment_count"] / primary["comment_count"], 3)
        if primary["comment_count"]
        else 0.0
    )
    primary["first_shot_comment"] = next(
        (str(row.get("first_shot_comment") or "") for row in group if row.get("first_shot_comment")),
        "",
    )
    primary["score"] = segment_battle_score(
        changes=changes,
        reverts=reverts,
        editors=editors,
        changed_word_count=changed_word_count,
        change_type=change_type,
    )
    primary["battle_group_size"] = len(group)
    primary["related_segments"] = [public_segment_row(row) for row in group]
    return public_segment_row(primary)


def public_segment_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not key.startswith("_")}


def segments_overlap(left: str, right: str) -> bool:
    left_norm = normalize_candidate(left)
    right_norm = normalize_candidate(right)
    if not left_norm or not right_norm:
        return False
    if left_norm in right_norm or right_norm in left_norm:
        return True
    left_words = set(WORD_RE.findall(left_norm))
    right_words = set(WORD_RE.findall(right_norm))
    if not left_words or not right_words:
        return False
    overlap = len(left_words & right_words) / min(len(left_words), len(right_words))
    return overlap >= 0.5


def is_revert_like(edit: dict[str, Any]) -> bool:
    comment = str(edit.get("comment") or "")
    if REVERT_RE.search(comment):
        return True
    tags = edit.get("tags") or []
    return any("revert" in str(tag).lower() or "rollback" in str(tag).lower() or "undo" in str(tag).lower() for tag in tags)


def revision_content(revision: dict[str, Any]) -> str:
    slots = revision.get("slots") or {}
    main = slots.get("main") or {}
    content = main.get("content")
    if content is None:
        content = revision.get("content") or revision.get("*")
    return str(content or "")


def period_bounds(period: str | None) -> tuple[datetime | None, datetime | None]:
    if not period:
        return None, None
    year_match = re.match(r"^history-year:\d{4}-\d{2}:(\d{4})$", period)
    if year_match:
        year = int(year_match.group(1))
        return datetime(year, 1, 1, tzinfo=timezone.utc), datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    matches = re.findall(r"\d{4}-\d{2}", period)
    if not matches:
        return None, None
    year, month = (int(part) for part in matches[-1].split("-"))
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def api_url_for_wiki(wiki: str) -> str:
    if wiki == settings.wiki_db:
        return f"https://{settings.wiki_server_name}/w/api.php"
    if wiki.endswith("wiki") and len(wiki) > 4:
        return f"https://{wiki[:-4]}.wikipedia.org/w/api.php"
    return f"https://{settings.wiki_server_name}/w/api.php"
