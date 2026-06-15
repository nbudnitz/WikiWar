from __future__ import annotations

import argparse
import bz2
from dataclasses import dataclass, field
from datetime import datetime
import gzip
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import shutil
import time
from typing import Any, Callable, Iterable
from xml.etree import ElementTree

import httpx

from .config import settings
from .controversy import build_local_evidence_payload
from .historical import (
    DownloadProgress,
    open_compressed_dump,
    parse_history_row,
    pick_preferred_dump_format,
    historical_year_scoreboard,
    rank_historical_rows,
    record_to_aggregate,
    score_historical_page,
)
from .repository import (
    historical_month_periods,
    historical_month_periods_for_year,
    historical_scoreboard,
    is_historical_year_period,
    load_historical_aggregates,
    save_historical_evidence,
)
from .schema import SessionLocal, init_db
from .segments import period_bounds


REVISION_DUMPS_BASE_URL = "https://dumps.wikimedia.org"
DEFAULT_EVIDENCE_DUMP_DIR = Path("data/evidence-dumps")
DEFAULT_EVIDENCE_CHECKPOINT_DIR = Path("data/evidence-checkpoints")
DEFAULT_EVIDENCE_STATUS_FILE = Path("data/logs/evidence-backfill-status.json")
HISTORICAL_MONTH_RE = re.compile(r"^history:(?P<snapshot>\d{4}-\d{2}):(?P<month>\d{4}-\d{2})$")
HISTORICAL_YEAR_RE = re.compile(r"^history-year:(?P<snapshot>\d{4}-\d{2}):(?P<year>\d{4})$")


@dataclass
class CandidateEvidenceInput:
    wiki: str
    page_id: int
    page_title: str
    period: str
    peak_score: float = 0.0
    revert_count: int = 0
    mutual_revert_pairs: int = 0
    talk_page_id: int | None = None


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


@dataclass(frozen=True)
class RevisionDumpShard:
    filename: str
    url: str
    start_page_id: int
    end_page_id: int


class DumpIndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.hrefs.append(href)


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
                talk_page_id=int(row["talk_page_id"]) if row.get("talk_page_id") else None,
            )
        )
    return result


def backfill_local_evidence(
    *,
    period: str,
    dump_paths: list[Path],
    wiki: str = settings.wiki_db,
    limit: int = 100,
    status_file: Path | None = None,
    status: dict[str, Any] | None = None,
    checkpoint_dir: Path | None = None,
) -> EvidenceBackfillResult:
    candidates = evidence_inputs_from_rows(candidate_rows_for_period(period, limit), period, wiki)
    collected = collect_revisions_from_xml_dumps(
        dump_paths,
        candidates,
        period,
        status_file=status_file,
        status=status,
        checkpoint_dir=checkpoint_dir,
    )
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
            if status_file and status:
                write_evidence_status(
                    status_file,
                    {
                        **status,
                        "phase": "writing_cache",
                        "pages_written": pages_written,
                        "pages_with_article_revisions": sum(1 for revisions in collected.articles.values() if revisions),
                    },
                )
    return EvidenceBackfillResult(
        period=period,
        candidates=len(candidates),
        pages_with_article_revisions=sum(1 for revisions in collected.articles.values() if revisions),
        pages_written=pages_written,
    )


def auto_backfill_local_evidence(
    *,
    periods: list[str],
    wiki: str = settings.wiki_db,
    limit: int = 20,
    output_dir: Path = DEFAULT_EVIDENCE_DUMP_DIR,
    checkpoint_dir: Path = DEFAULT_EVIDENCE_CHECKPOINT_DIR,
    history_dump_dir: Path = Path("data/dumps"),
    status_file: Path = DEFAULT_EVIDENCE_STATUS_FILE,
    include_talk: bool = False,
    reset_checkpoints: bool = False,
) -> list[EvidenceBackfillResult]:
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    status_file.parent.mkdir(parents=True, exist_ok=True)
    write_evidence_status(
        status_file,
        {
            "status": "running",
            "phase": "starting",
            "periods": periods,
            "periods_total": len(periods),
            "periods_done": 0,
            "limit": limit,
            "wiki": wiki,
            "output_dir": str(output_dir),
            "checkpoint_dir": str(checkpoint_dir),
            "include_talk": include_talk,
        },
    )

    results: list[EvidenceBackfillResult] = []
    for period_index, period in enumerate(periods, start=1):
        snapshot = snapshot_from_period(period)
        if not snapshot:
            raise ValueError(f"Cannot determine snapshot from period: {period}")
        write_evidence_status(
            status_file,
            {
                "status": "running",
                "phase": "resolving_candidates",
                "periods": periods,
                "periods_total": len(periods),
                "periods_done": period_index - 1,
                "current_period": period,
                "limit": limit,
                "wiki": wiki,
                "include_talk": include_talk,
            },
        )
        candidates = evidence_inputs_from_rows(candidate_rows_for_period(period, limit), period, wiki)

        status_base = {
            "status": "running",
            "periods": periods,
            "periods_total": len(periods),
            "periods_done": period_index - 1,
            "current_period": period,
            "candidates_total": len(candidates),
            "limit": limit,
            "wiki": wiki,
            "include_talk": include_talk,
        }
        if include_talk:
            write_evidence_status(status_file, {**status_base, "phase": "resolving_talk_pages"})
        talk_page_ids = candidate_talk_page_ids(candidates) if include_talk else {}
        missing_talk_candidates = [
            candidate for candidate in candidates if candidate.page_id not in talk_page_ids
        ]
        if include_talk and missing_talk_candidates:
            talk_page_ids.update(
                talk_page_ids_from_history_dumps(
                    missing_talk_candidates,
                    period=period,
                    wiki=wiki,
                    history_dump_dir=history_dump_dir,
                    status_file=status_file,
                    status={**status_base, "phase": "resolving_talk_pages"},
                )
            )
        target_page_ids = {candidate.page_id for candidate in candidates}
        target_page_ids.update(talk_page_ids.values())
        shards = revision_shards_for_page_ids(
            list_revision_dump_shards(snapshot=snapshot, wiki=wiki),
            target_page_ids,
        )
        write_evidence_status(
            status_file,
            {
                "status": "running",
                "phase": "downloading_shards",
                "periods": periods,
                "periods_total": len(periods),
                "periods_done": period_index - 1,
                "current_period": period,
                "candidates_total": len(candidates),
                "talk_pages_found": len(talk_page_ids),
                "target_page_ids": len(target_page_ids),
                "shards_total": len(shards),
                "shards_done": 0,
                "limit": limit,
                "wiki": wiki,
                "include_talk": include_talk,
            },
        )
        dump_paths: list[Path] = []
        for shard_index, shard in enumerate(shards, start=1):
            path = download_revision_dump_shard(
                shard,
                output_dir=output_dir,
                status_file=status_file,
                status={
                    "status": "running",
                    "phase": "downloading_shards",
                    "periods": periods,
                    "periods_total": len(periods),
                    "periods_done": period_index - 1,
                    "current_period": period,
                    "candidates_total": len(candidates),
                    "talk_pages_found": len(talk_page_ids),
                    "target_page_ids": len(target_page_ids),
                    "shards_total": len(shards),
                    "shards_done": shard_index - 1,
                    "current_shard": shard.filename,
                    "limit": limit,
                    "wiki": wiki,
                    "include_talk": include_talk,
                },
            )
            dump_paths.append(path)
            write_evidence_status(
                status_file,
                {
                    "status": "running",
                    "phase": "downloading_shards",
                    "periods": periods,
                    "periods_total": len(periods),
                    "periods_done": period_index - 1,
                    "current_period": period,
                    "candidates_total": len(candidates),
                    "talk_pages_found": len(talk_page_ids),
                    "target_page_ids": len(target_page_ids),
                    "shards_total": len(shards),
                    "shards_done": shard_index,
                    "current_shard": shard.filename,
                    "limit": limit,
                    "wiki": wiki,
                    "include_talk": include_talk,
                },
            )

        write_evidence_status(
            status_file,
            {
                "status": "running",
                "phase": "parsing_shards",
                "periods": periods,
                "periods_total": len(periods),
                "periods_done": period_index - 1,
                "current_period": period,
                "candidates_total": len(candidates),
                "talk_pages_found": len(talk_page_ids),
                "target_page_ids": len(target_page_ids),
                "shards_total": len(shards),
                "shards_done": len(shards),
                "limit": limit,
                "wiki": wiki,
                "include_talk": include_talk,
            },
        )
        result = backfill_local_evidence(
            period=period,
            dump_paths=dump_paths,
            wiki=wiki,
            limit=limit,
            status_file=status_file,
            status={
                "status": "running",
                "phase": "parsing_shards",
                "periods": periods,
                "periods_total": len(periods),
                "periods_done": period_index - 1,
                "current_period": period,
                "candidates_total": len(candidates),
                "talk_pages_found": len(talk_page_ids),
                "target_page_ids": len(target_page_ids),
                "shards_total": len(shards),
                "shards_done": len(shards),
                "limit": limit,
                "wiki": wiki,
                "include_talk": include_talk,
            },
            checkpoint_dir=prepare_period_checkpoint_dir(
                checkpoint_dir,
                wiki=wiki,
                period=period,
                reset=reset_checkpoints,
            ),
        )
        results.append(result)
        write_evidence_status(
            status_file,
            {
                "status": "running",
                "phase": "period_done",
                "periods": periods,
                "periods_total": len(periods),
                "periods_done": period_index,
                "current_period": period,
                "candidates_total": result.candidates,
                "pages_with_article_revisions": result.pages_with_article_revisions,
                "pages_written": result.pages_written,
                "shards_total": len(shards),
                "shards_done": len(shards),
                "limit": limit,
                "wiki": wiki,
                "include_talk": include_talk,
            },
        )

    write_evidence_status(
        status_file,
        {
            "status": "complete",
            "phase": "complete",
            "periods": periods,
            "periods_total": len(periods),
            "periods_done": len(periods),
            "results": [result.__dict__ for result in results],
            "limit": limit,
            "wiki": wiki,
            "include_talk": include_talk,
        },
    )
    return results


def snapshot_from_period(period: str) -> str:
    month_match = HISTORICAL_MONTH_RE.match(period)
    if month_match:
        return month_match.group("snapshot")
    year_match = HISTORICAL_YEAR_RE.match(period)
    if year_match:
        return year_match.group("snapshot")
    return ""


def xml_snapshot_id(snapshot: str) -> str:
    match = re.match(r"^(\d{4})-(\d{2})$", snapshot)
    if not match:
        raise ValueError(f"Snapshot must be YYYY-MM: {snapshot}")
    return f"{match.group(1)}{match.group(2)}01"


def list_revision_dump_shards(*, snapshot: str, wiki: str = settings.wiki_db) -> list[RevisionDumpShard]:
    snapshot_id = xml_snapshot_id(snapshot)
    base_url = f"{REVISION_DUMPS_BASE_URL}/{wiki}/{snapshot_id}/"
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0) as client:
        response = client.get(base_url)
        response.raise_for_status()
        index_html = response.text
    parser = DumpIndexParser()
    parser.feed(index_html)
    # Match pages-meta-history shards in any compression format, then prefer the
    # smallest available (7z is typically an order of magnitude smaller than bz2).
    candidate_pattern = re.compile(
        rf"^{re.escape(wiki)}-{re.escape(snapshot_id)}-pages-meta-history\d+\.xml-"
        r"p(?P<start>\d+)p(?P<end>\d+)"
    )
    base_group_pattern = re.compile(
        rf"^{re.escape(wiki)}-{re.escape(snapshot_id)}-pages-meta-history\d+\.xml-"
        r"p(?P<start>\d+)p(?P<end>\d+)$"
    )
    seen: set[str] = set()
    all_filenames: list[str] = []
    page_ids_by_base: dict[str, tuple[int, int]] = {}
    for href in parser.hrefs:
        filename = href.rsplit("/", 1)[-1]
        if filename in seen:
            continue
        seen.add(filename)
        match = candidate_pattern.match(filename)
        if not match:
            continue
        all_filenames.append(filename)
        # Strip the compression suffix to get the grouping base name.
        stripped = filename
        for suffix in (".7z", ".bz2", ".gz"):
            if stripped.endswith(suffix):
                stripped = stripped[: -len(suffix)]
                break
        page_ids_by_base[stripped] = (int(match.group("start")), int(match.group("end")))
    preferred = pick_preferred_dump_format(
        all_filenames,
        base_pattern=base_group_pattern,
        prefer=(".7z", ".bz2", ".gz"),
    )
    shards: list[RevisionDumpShard] = []
    for filename in preferred:
        stripped = filename
        for suffix in (".7z", ".bz2", ".gz"):
            if stripped.endswith(suffix):
                stripped = stripped[: -len(suffix)]
                break
        page_range = page_ids_by_base.get(stripped)
        if not page_range:
            continue
        start_page_id, end_page_id = page_range
        shards.append(
            RevisionDumpShard(
                filename=filename,
                url=f"{base_url}{filename}",
                start_page_id=start_page_id,
                end_page_id=end_page_id,
            )
        )
    shards.sort(key=lambda shard: (shard.start_page_id, shard.end_page_id, shard.filename))
    return shards


def revision_shards_for_page_ids(
    shards: list[RevisionDumpShard],
    page_ids: Iterable[int],
) -> list[RevisionDumpShard]:
    selected: dict[str, RevisionDumpShard] = {}
    for page_id in sorted({int(page_id) for page_id in page_ids if int(page_id) > 0}):
        shard = revision_shard_for_page_id(shards, page_id)
        if shard:
            selected[shard.filename] = shard
    return sorted(selected.values(), key=lambda shard: (shard.start_page_id, shard.end_page_id, shard.filename))


def revision_shard_for_page_id(shards: list[RevisionDumpShard], page_id: int) -> RevisionDumpShard | None:
    for shard in shards:
        if shard.start_page_id <= page_id <= shard.end_page_id:
            return shard
    return None


def download_revision_dump_shard(
    shard: RevisionDumpShard,
    *,
    output_dir: Path,
    status_file: Path | None = None,
    status: dict[str, Any] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / shard.filename
    if target.exists() and target.stat().st_size > 0:
        return target

    partial = target.with_suffix(target.suffix + ".part")
    resume_from = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": settings.user_agent}
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"

    downloaded = resume_from
    total_bytes = 0
    last_status_at = 0.0
    with httpx.Client(headers=headers, timeout=None, follow_redirects=True) as client:
        with client.stream("GET", shard.url) as response:
            response.raise_for_status()
            if response.status_code != 206 and resume_from:
                resume_from = 0
                downloaded = 0
            total_header = response.headers.get("Content-Length")
            if total_header and total_header.isdigit():
                total_bytes = int(total_header) + (resume_from if response.status_code == 206 else 0)
            mode = "ab" if response.status_code == 206 and resume_from else "wb"
            with partial.open(mode) as file, DownloadProgress(
                shard.filename, total_bytes=total_bytes or None
            ) as progress:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    file.write(chunk)
                    downloaded += len(chunk)
                    progress.advance(len(chunk))
                    now = time.monotonic()
                    if status_file and status and now - last_status_at >= 5:
                        last_status_at = now
                        write_evidence_status(
                            status_file,
                            {
                                **status,
                                "current_shard_downloaded_bytes": downloaded,
                                "current_shard_total_bytes": total_bytes,
                            },
                        )
    partial.replace(target)
    return target


def talk_page_ids_from_history_dumps(
    candidates: list[CandidateEvidenceInput],
    *,
    period: str,
    wiki: str,
    history_dump_dir: Path,
    status_file: Path | None = None,
    status: dict[str, Any] | None = None,
) -> dict[int, int]:
    candidate_by_title = {normalize_title(candidate.page_title): candidate for candidate in candidates}
    if not candidate_by_title:
        return {}
    paths = history_dump_paths_for_period(period=period, wiki=wiki, history_dump_dir=history_dump_dir)
    result: dict[int, int] = {}
    for path_index, path in enumerate(paths, start=1):
        if not path.exists():
            continue
        rows_seen = 0
        last_status_at = 0.0
        with bz2.open(path, "rt", encoding="utf-8", newline="") as file:
            for line in file:
                rows_seen += 1
                now = time.monotonic()
                if status_file and status and now - last_status_at >= 5:
                    last_status_at = now
                    write_evidence_status(
                        status_file,
                        {
                            **status,
                            "phase": "resolving_talk_pages",
                            "history_dumps_done": path_index - 1,
                            "history_dumps_total": len(paths),
                            "current_history_dump": path.name,
                            "current_history_rows_seen": rows_seen,
                            "talk_pages_found": len(result),
                        },
                    )
                revision = parse_history_row(line.rstrip("\n").split("\t"), namespace=1)
                if revision is None:
                    continue
                candidate = candidate_by_title.get(normalize_title(revision.page_title))
                if not candidate:
                    continue
                result.setdefault(candidate.page_id, revision.page_id)
                if len(result) == len(candidate_by_title):
                    return result
        if status_file and status:
            write_evidence_status(
                status_file,
                {
                    **status,
                    "phase": "resolving_talk_pages",
                    "history_dumps_done": path_index,
                    "history_dumps_total": len(paths),
                    "current_history_dump": path.name,
                    "current_history_rows_seen": rows_seen,
                    "talk_pages_found": len(result),
                },
            )
    return result


def candidate_talk_page_ids(candidates: list[CandidateEvidenceInput]) -> dict[int, int]:
    return {
        candidate.page_id: int(candidate.talk_page_id)
        for candidate in candidates
        if candidate.talk_page_id
    }


def history_dump_paths_for_period(*, period: str, wiki: str, history_dump_dir: Path) -> list[Path]:
    month_match = HISTORICAL_MONTH_RE.match(period)
    if month_match:
        snapshot = month_match.group("snapshot")
        month = month_match.group("month")
        return [history_dump_dir / f"{snapshot}.{wiki}.{month}.tsv.bz2"]

    if is_historical_year_period(period):
        with SessionLocal() as session:
            months = historical_month_periods_for_year(historical_month_periods(session), period)
        return [history_dump_paths_for_period(period=month, wiki=wiki, history_dump_dir=history_dump_dir)[0] for month in months]
    return []


def write_evidence_status(status_file: Path, payload: dict[str, Any]) -> None:
    status_file.parent.mkdir(parents=True, exist_ok=True)
    status = {
        **payload,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    temporary = status_file.with_suffix(status_file.suffix + ".tmp")
    temporary.write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(status_file)


def read_evidence_status(status_file: Path = DEFAULT_EVIDENCE_STATUS_FILE) -> dict[str, Any]:
    if not status_file.exists():
        return {
            "status": "not_started",
            "phase": "not_started",
            "message": "No evidence backfill status file exists yet.",
        }
    try:
        return json.loads(status_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "status": "unreadable",
            "phase": "unreadable",
            "message": str(exc),
        }


def prepare_period_checkpoint_dir(
    checkpoint_root: Path,
    *,
    wiki: str,
    period: str,
    reset: bool = False,
) -> Path:
    path = checkpoint_root / safe_path_part(wiki) / safe_path_part(period)
    if reset and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def shard_checkpoint_path(checkpoint_dir: Path, dump_path: Path) -> Path:
    return checkpoint_dir / f"{safe_path_part(dump_path.name)}.json"


def safe_path_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def load_shard_checkpoint(path: Path) -> CollectedEvidenceRevisions | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    collected = CollectedEvidenceRevisions()
    collected.articles = {
        int(page_id): list(revisions)
        for page_id, revisions in (payload.get("articles") or {}).items()
    }
    collected.talks = {
        int(page_id): list(revisions)
        for page_id, revisions in (payload.get("talks") or {}).items()
    }
    return collected


def write_shard_checkpoint(path: Path, collected: CollectedEvidenceRevisions) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "articles": {
            str(page_id): revisions
            for page_id, revisions in sorted(collected.articles.items())
        },
        "talks": {
            str(page_id): revisions
            for page_id, revisions in sorted(collected.talks.items())
        },
        "written_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    temporary.replace(path)


def merge_collected_revisions(
    target: CollectedEvidenceRevisions,
    source: CollectedEvidenceRevisions,
) -> None:
    for page_id, revisions in source.articles.items():
        target.articles.setdefault(page_id, []).extend(revisions)
    for page_id, revisions in source.talks.items():
        target.talks.setdefault(page_id, []).extend(revisions)


def collect_revisions_from_xml_dumps(
    dump_paths: list[Path],
    candidates: list[CandidateEvidenceInput],
    period: str,
    *,
    status_file: Path | None = None,
    status: dict[str, Any] | None = None,
    checkpoint_dir: Path | None = None,
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
    candidates_by_talk_id = {
        int(candidate.talk_page_id): candidate
        for candidate in candidates
        if candidate.talk_page_id
    }
    collected = CollectedEvidenceRevisions()
    checkpoint_shards_reused = 0
    checkpoint_shards_written = 0
    for path_index, path in enumerate(dump_paths, start=1):
        checkpoint_path = shard_checkpoint_path(checkpoint_dir, path) if checkpoint_dir else None
        checkpoint = load_shard_checkpoint(checkpoint_path) if checkpoint_path else None
        if checkpoint is not None:
            checkpoint_shards_reused += 1
            merge_collected_revisions(collected, checkpoint)
            if status_file and status:
                write_evidence_status(
                    status_file,
                    {
                        **status,
                        "phase": "parsing_shards",
                        "parse_shards_done": path_index,
                        "parse_shards_total": len(dump_paths),
                        "current_parse_shard": path.name,
                        "current_parse_pages_seen": 0,
                        "current_parse_revisions_seen": 0,
                        "article_pages_found": len(collected.articles),
                        "talk_pages_found_in_xml": len(collected.talks),
                        "checkpoint_shards_reused": checkpoint_shards_reused,
                        "checkpoint_shards_written": checkpoint_shards_written,
                    },
                )
            continue

        shard_collected = CollectedEvidenceRevisions()
        pages_seen = 0
        last_status_at = 0.0

        revisions_seen = 0

        def report_parse_progress(scanned_pages: int, scanned_revisions: int) -> None:
            nonlocal pages_seen, revisions_seen, last_status_at
            pages_seen = scanned_pages
            revisions_seen = scanned_revisions
            now = time.monotonic()
            if not status_file or not status or now - last_status_at < 5:
                return
            last_status_at = now
            write_evidence_status(
                status_file,
                {
                    **status,
                    "phase": "parsing_shards",
                    "parse_shards_done": path_index - 1,
                    "parse_shards_total": len(dump_paths),
                    "current_parse_shard": path.name,
                    "current_parse_pages_seen": pages_seen,
                    "current_parse_revisions_seen": revisions_seen,
                    "article_pages_found": len(collected.articles) + len(shard_collected.articles),
                    "talk_pages_found_in_xml": len(collected.talks) + len(shard_collected.talks),
                    "checkpoint_shards_reused": checkpoint_shards_reused,
                    "checkpoint_shards_written": checkpoint_shards_written,
                },
            )

        for page in iter_target_mediawiki_xml_pages(
            path,
            candidates_by_id=candidates_by_id,
            candidates_by_title=candidates_by_title,
            candidates_by_talk_title=candidates_by_talk_title,
            candidates_by_talk_id=candidates_by_talk_id,
            progress_callback=report_parse_progress,
        ):
            namespace = int(page.get("namespace") or 0)
            candidate = page["candidate"]
            if namespace == 0:
                revisions = revisions_in_period(page.get("revisions") or [], start, end)
                if revisions:
                    shard_collected.articles.setdefault(candidate.page_id, []).extend(revisions)
            elif namespace == 1:
                revisions = revisions_in_period(page.get("revisions") or [], start, end)
                if revisions:
                    shard_collected.talks.setdefault(candidate.page_id, []).extend(revisions)
        merge_collected_revisions(collected, shard_collected)
        if checkpoint_path:
            write_shard_checkpoint(checkpoint_path, shard_collected)
            checkpoint_shards_written += 1
        if status_file and status:
            write_evidence_status(
                status_file,
                {
                    **status,
                    "phase": "parsing_shards",
                    "parse_shards_done": path_index,
                    "parse_shards_total": len(dump_paths),
                    "current_parse_shard": path.name,
                    "current_parse_pages_seen": pages_seen,
                    "current_parse_revisions_seen": revisions_seen,
                    "article_pages_found": len(collected.articles),
                    "talk_pages_found_in_xml": len(collected.talks),
                    "checkpoint_shards_reused": checkpoint_shards_reused,
                    "checkpoint_shards_written": checkpoint_shards_written,
                },
            )
    for revisions in collected.articles.values():
        revisions.sort(key=lambda item: (str(item.get("timestamp") or ""), int(item.get("rev_id") or 0)))
    for revisions in collected.talks.values():
        revisions.sort(key=lambda item: (str(item.get("timestamp") or ""), int(item.get("rev_id") or 0)))
    return collected


def iter_target_mediawiki_xml_pages(
    path: Path,
    *,
    candidates_by_id: dict[int, CandidateEvidenceInput],
    candidates_by_title: dict[str, CandidateEvidenceInput],
    candidates_by_talk_title: dict[str, CandidateEvidenceInput],
    candidates_by_talk_id: dict[int, CandidateEvidenceInput] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> Iterable[dict[str, Any]]:
    with open_dump_text(path) as file:
        context = ElementTree.iterparse(file, events=("start", "end"))
        stack: list[str] = []
        pages_seen = 0
        revisions_seen = 0
        current: dict[str, Any] | None = None

        def resolve_current_candidate() -> CandidateEvidenceInput | None:
            if current is None:
                return None
            candidate = current.get("candidate")
            if candidate:
                return candidate
            namespace = int(current.get("namespace") or 0)
            page_id = int(current.get("page_id") or 0)
            normalized_title = normalize_title(str(current.get("title") or ""))
            if namespace == 0:
                candidate = candidates_by_id.get(page_id) or candidates_by_title.get(normalized_title)
            elif namespace == 1:
                candidate = (candidates_by_talk_id or {}).get(page_id) or candidates_by_talk_title.get(normalized_title)
            if candidate:
                current["candidate"] = candidate
            return candidate

        for event, element in context:
            name = local_name(element.tag)
            if event == "start":
                stack.append(name)
                if name == "page":
                    current = {
                        "title": "",
                        "namespace": 0,
                        "page_id": 0,
                        "candidate": None,
                        "revisions": [],
                    }
                continue

            parent = stack[-2] if len(stack) >= 2 else ""
            if current is not None:
                if name == "title" and parent == "page":
                    current["title"] = element.text or ""
                    resolve_current_candidate()
                elif name == "ns" and parent == "page":
                    current["namespace"] = int(element.text or 0)
                    resolve_current_candidate()
                elif name == "id" and parent == "page" and not current.get("page_id"):
                    current["page_id"] = int(element.text or 0)
                    resolve_current_candidate()
                elif name == "revision":
                    revisions_seen += 1
                    if resolve_current_candidate():
                        current["revisions"].append(parse_revision_element(element))
                    element.clear()
                    if progress_callback:
                        progress_callback(pages_seen, revisions_seen)
                elif name == "page":
                    pages_seen += 1
                    candidate = resolve_current_candidate()
                    if progress_callback:
                        progress_callback(pages_seen, revisions_seen)
                    if candidate:
                        yield {
                            "title": current.get("title") or "",
                            "namespace": int(current.get("namespace") or 0),
                            "page_id": int(current.get("page_id") or 0),
                            "candidate": candidate,
                            "revisions": list(current.get("revisions") or []),
                        }
                    element.clear()
                    current = None
            if stack:
                stack.pop()


def iter_mediawiki_xml_pages(path: Path) -> Iterable[dict[str, Any]]:
    with open_dump_text(path) as file:
        context = ElementTree.iterparse(file, events=("end",))
        for _, element in context:
            if local_name(element.tag) != "page":
                continue
            yield parse_page_element(element)
            element.clear()


def page_element_identity(element: ElementTree.Element) -> tuple[str, int, int]:
    title = ""
    namespace = 0
    page_id = 0
    for child in element:
        name = local_name(child.tag)
        if name == "title":
            title = child.text or ""
        elif name == "ns":
            namespace = int(child.text or 0)
        elif name == "id" and not page_id:
            page_id = int(child.text or 0)
    return title, namespace, page_id


def parse_page_element(element: ElementTree.Element) -> dict[str, Any]:
    title, namespace, page_id = page_element_identity(element)
    return {
        "title": title,
        "namespace": namespace,
        "page_id": page_id,
        "revisions": parse_page_revisions(element),
    }


def parse_page_revisions(element: ElementTree.Element) -> list[dict[str, Any]]:
    revisions: list[dict[str, Any]] = []
    for child in element:
        name = local_name(child.tag)
        if name == "revision":
            revisions.append(parse_revision_element(child))
    return revisions


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
    # Defer to the shared opener so .7z (via the 7z binary) is supported alongside gz/bz2.
    return open_compressed_dump(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build local historical battle/talk evidence from XML revision dumps.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill_parser = subparsers.add_parser("backfill", help="Backfill local evidence cache for a historical period.")
    backfill_parser.add_argument("--period", required=True)
    backfill_parser.add_argument("--wiki", default=settings.wiki_db)
    backfill_parser.add_argument("--limit", type=int, default=100)
    backfill_parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_EVIDENCE_CHECKPOINT_DIR)
    backfill_parser.add_argument(
        "--reset-checkpoints",
        action="store_true",
        help="Delete saved evidence checkpoints for this period before parsing.",
    )
    backfill_parser.add_argument(
        "--dump",
        dest="dumps",
        type=Path,
        action="append",
        required=True,
        help="Local MediaWiki pages-meta-history XML dump. Pass multiple times for article and talk dump shards.",
    )

    auto_parser = subparsers.add_parser(
        "auto-backfill",
        help="Download needed revision XML shards and backfill local evidence for selected historical periods.",
    )
    auto_parser.add_argument(
        "--period",
        dest="periods",
        action="append",
        required=True,
        help="Historical period to populate. Pass multiple times for multiple years/months.",
    )
    auto_parser.add_argument("--wiki", default=settings.wiki_db)
    auto_parser.add_argument("--limit", type=int, default=20)
    auto_parser.add_argument("--output-dir", type=Path, default=DEFAULT_EVIDENCE_DUMP_DIR)
    auto_parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_EVIDENCE_CHECKPOINT_DIR)
    auto_parser.add_argument("--history-dump-dir", type=Path, default=Path("data/dumps"))
    auto_parser.add_argument("--status-file", type=Path, default=DEFAULT_EVIDENCE_STATUS_FILE)
    auto_parser.add_argument(
        "--include-talk",
        action="store_true",
        help="Also scan local mediawiki_history TSVs for talk-page IDs and download their XML shards.",
    )
    auto_parser.add_argument(
        "--reset-checkpoints",
        action="store_true",
        help="Delete saved evidence checkpoints for each period before parsing.",
    )

    status_parser = subparsers.add_parser("status", help="Print the latest evidence backfill status JSON.")
    status_parser.add_argument("--status-file", type=Path, default=DEFAULT_EVIDENCE_STATUS_FILE)

    args = parser.parse_args()
    if args.command == "backfill":
        result = backfill_local_evidence(
            period=args.period,
            dump_paths=args.dumps,
            wiki=args.wiki,
            limit=args.limit,
            checkpoint_dir=prepare_period_checkpoint_dir(
                args.checkpoint_dir,
                wiki=args.wiki,
                period=args.period,
                reset=args.reset_checkpoints,
            ),
        )
        print(
            "period={period} candidates={candidates} pages_with_article_revisions={pages_with_article_revisions} "
            "pages_written={pages_written}".format(**result.__dict__)
        )
    elif args.command == "auto-backfill":
        results = auto_backfill_local_evidence(
            periods=args.periods,
            wiki=args.wiki,
            limit=args.limit,
            output_dir=args.output_dir,
            checkpoint_dir=args.checkpoint_dir,
            history_dump_dir=args.history_dump_dir,
            status_file=args.status_file,
            include_talk=args.include_talk,
            reset_checkpoints=args.reset_checkpoints,
        )
        for result in results:
            print(
                "period={period} candidates={candidates} pages_with_article_revisions={pages_with_article_revisions} "
                "pages_written={pages_written}".format(**result.__dict__)
            )
    elif args.command == "status":
        print(json.dumps(read_evidence_status(args.status_file), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
