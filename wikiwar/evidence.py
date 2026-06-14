from __future__ import annotations

import argparse
import bz2
from dataclasses import dataclass, field
from datetime import datetime
import gzip
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree

from .config import settings
from .controversy import build_local_evidence_payload
from .historical import (
    historical_year_scoreboard,
    rank_historical_rows,
    record_to_aggregate,
    score_historical_page,
)
from .repository import (
    historical_scoreboard,
    is_historical_year_period,
    load_historical_aggregates,
    save_historical_evidence,
)
from .schema import SessionLocal, init_db
from .segments import period_bounds


@dataclass
class CandidateEvidenceInput:
    wiki: str
    page_id: int
    page_title: str
    period: str
    peak_score: float = 0.0
    revert_count: int = 0
    mutual_revert_pairs: int = 0


@dataclass
class CollectedEvidenceRevisions:
    articles: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    talks: dict[int, list[dict[str, Any]]] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceBackfillResult:
    period: str
    candidates: int
    pages_with_article_revisions: int
    pages_written: int


def candidate_rows_for_period(period: str, limit: int) -> list[dict[str, Any]]:
    init_db()
    with SessionLocal() as session:
        if is_historical_year_period(period):
            return historical_year_scoreboard(session, period=period, limit=limit)

        aggregates = [record_to_aggregate(record) for record in load_historical_aggregates(session, period)]
        scored = [score_historical_page(aggregate) for aggregate in aggregates]
        selected = rank_historical_rows(scored, limit)
        if selected:
            return selected
        return historical_scoreboard(session, period, limit)


def evidence_inputs_from_rows(rows: list[dict[str, Any]], period: str, wiki: str) -> list[CandidateEvidenceInput]:
    result: list[CandidateEvidenceInput] = []
    for row in rows:
        page_id = int(row.get("page_id") or 0)
        page_title = str(row.get("page_title") or "")
        if not page_id or not page_title:
            continue
        result.append(
            CandidateEvidenceInput(
                wiki=str(row.get("wiki") or wiki),
                page_id=page_id,
                page_title=page_title,
                period=period,
                peak_score=float(row.get("peak_score") or 0.0),
                revert_count=int(row.get("revert_count") or 0),
                mutual_revert_pairs=int(row.get("mutual_revert_pairs") or 0),
            )
        )
    return result


def backfill_local_evidence(
    *,
    period: str,
    dump_paths: list[Path],
    wiki: str = settings.wiki_db,
    limit: int = 100,
) -> EvidenceBackfillResult:
    candidates = evidence_inputs_from_rows(candidate_rows_for_period(period, limit), period, wiki)
    collected = collect_revisions_from_xml_dumps(dump_paths, candidates, period)
    pages_written = 0
    init_db()
    with SessionLocal() as session:
        for candidate in candidates:
            article_revisions = collected.articles.get(candidate.page_id, [])
            if not article_revisions:
                continue
            payload = build_local_evidence_payload(
                article_revisions=article_revisions,
                talk_revisions=collected.talks.get(candidate.page_id, []),
                wiki=candidate.wiki,
                page_id=candidate.page_id,
                page_title=candidate.page_title,
                period=period,
                peak_score=candidate.peak_score,
                revert_count=candidate.revert_count,
                mutual_revert_pairs=candidate.mutual_revert_pairs,
            )
            save_historical_evidence(
                session,
                period=period,
                wiki=candidate.wiki,
                page_id=candidate.page_id,
                page_title=candidate.page_title,
                source=str(payload.get("source") or "local_revision_dump"),
                payload=payload,
            )
            pages_written += 1
    return EvidenceBackfillResult(
        period=period,
        candidates=len(candidates),
        pages_with_article_revisions=sum(1 for revisions in collected.articles.values() if revisions),
        pages_written=pages_written,
    )


def collect_revisions_from_xml_dumps(
    dump_paths: list[Path],
    candidates: list[CandidateEvidenceInput],
    period: str,
) -> CollectedEvidenceRevisions:
    start, end = period_bounds(period)
    if start is None or end is None:
        raise ValueError(f"Cannot determine bounds for period: {period}")

    candidates_by_id = {candidate.page_id: candidate for candidate in candidates}
    candidates_by_title = {normalize_title(candidate.page_title): candidate for candidate in candidates}
    candidates_by_talk_title = {
        normalize_title(f"Talk:{candidate.page_title.replace('_', ' ')}"): candidate
        for candidate in candidates
    }
    collected = CollectedEvidenceRevisions()
    for path in dump_paths:
        for page in iter_mediawiki_xml_pages(path):
            title = str(page.get("title") or "")
            namespace = int(page.get("namespace") or 0)
            page_id = int(page.get("page_id") or 0)
            normalized_title = normalize_title(title)
            if namespace == 0:
                candidate = candidates_by_id.get(page_id) or candidates_by_title.get(normalized_title)
                if not candidate:
                    continue
                revisions = revisions_in_period(page.get("revisions") or [], start, end)
                if revisions:
                    collected.articles.setdefault(candidate.page_id, []).extend(revisions)
            elif namespace == 1:
                candidate = candidates_by_talk_title.get(normalized_title)
                if not candidate:
                    continue
                revisions = revisions_in_period(page.get("revisions") or [], start, end)
                if revisions:
                    collected.talks.setdefault(candidate.page_id, []).extend(revisions)
    for revisions in collected.articles.values():
        revisions.sort(key=lambda item: (str(item.get("timestamp") or ""), int(item.get("rev_id") or 0)))
    for revisions in collected.talks.values():
        revisions.sort(key=lambda item: (str(item.get("timestamp") or ""), int(item.get("rev_id") or 0)))
    return collected


def iter_mediawiki_xml_pages(path: Path) -> Iterable[dict[str, Any]]:
    with open_dump_text(path) as file:
        context = ElementTree.iterparse(file, events=("end",))
        for _, element in context:
            if local_name(element.tag) != "page":
                continue
            yield parse_page_element(element)
            element.clear()


def parse_page_element(element: ElementTree.Element) -> dict[str, Any]:
    title = ""
    namespace = 0
    page_id = 0
    revisions: list[dict[str, Any]] = []
    for child in element:
        name = local_name(child.tag)
        if name == "title":
            title = child.text or ""
        elif name == "ns":
            namespace = int(child.text or 0)
        elif name == "id" and not page_id:
            page_id = int(child.text or 0)
        elif name == "revision":
            revisions.append(parse_revision_element(child))
    return {
        "title": title,
        "namespace": namespace,
        "page_id": page_id,
        "revisions": revisions,
    }


def parse_revision_element(element: ElementTree.Element) -> dict[str, Any]:
    revision: dict[str, Any] = {
        "rev_id": 0,
        "timestamp": "",
        "user_text": "",
        "comment": "",
        "content": "",
        "tags": [],
    }
    for child in element:
        name = local_name(child.tag)
        if name == "id" and not revision["rev_id"]:
            revision["rev_id"] = int(child.text or 0)
        elif name == "timestamp":
            revision["timestamp"] = child.text or ""
        elif name == "comment":
            revision["comment"] = child.text or ""
        elif name == "text":
            revision["content"] = child.text or ""
        elif name == "contributor":
            revision["user_text"] = contributor_name(child)
    return revision


def contributor_name(element: ElementTree.Element) -> str:
    for child in element:
        name = local_name(child.tag)
        if name in {"username", "ip"}:
            return child.text or ""
    return ""


def revisions_in_period(
    revisions: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    result = []
    for revision in revisions:
        timestamp = parse_xml_timestamp(str(revision.get("timestamp") or ""))
        if timestamp is None or timestamp < start or timestamp >= end:
            continue
        if not revision.get("content"):
            continue
        result.append(revision)
    return result


def parse_xml_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_title(value: str) -> str:
    return (value or "").strip().replace(" ", "_")


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def open_dump_text(path: Path):
    if path.suffix == ".bz2":
        return bz2.open(path, "rb")
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build local historical battle/talk evidence from XML revision dumps.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill local evidence cache for a historical period.")
    backfill_parser.add_argument("--period", required=True)
    backfill_parser.add_argument("--wiki", default=settings.wiki_db)
    backfill_parser.add_argument("--limit", type=int, default=100)
    backfill_parser.add_argument(
        "--dump",
        dest="dumps",
        type=Path,
        action="append",
        required=True,
        help="Local MediaWiki pages-meta-history XML dump. Pass multiple times for article and talk dump shards.",
    )

    args = parser.parse_args()
    if args.command == "backfill":
        result = backfill_local_evidence(
            period=args.period,
            dump_paths=args.dumps,
            wiki=args.wiki,
            limit=args.limit,
        )
        print(
            "period={period} candidates={candidates} pages_with_article_revisions={pages_with_article_revisions} "
            "pages_written={pages_written}".format(**result.__dict__)
        )


if __name__ == "__main__":
    main()
