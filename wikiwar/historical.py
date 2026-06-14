from __future__ import annotations

import argparse
import bz2
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from datetime import timedelta
import gzip
from html.parser import HTMLParser
import math
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Iterable
import xml.etree.ElementTree as ET

import httpx

from .config import settings
from .repository import (
    historical_aggregate_period_exists,
    historical_aggregate_periods,
    historical_processed_period_exists,
    load_page_admin_signals,
    load_page_admin_title_signals,
    load_historical_year_page_stats,
    load_historical_year_aggregates,
    load_historical_aggregates,
    mark_historical_period_processed,
    replace_historical_aggregates,
    replace_page_admin_signals,
    replace_page_admin_title_signals,
    replace_scoreboard_snapshot,
)
from .schema import SessionLocal, init_db
DUMPS_BASE_URL = "https://dumps.wikimedia.org/other/mediawiki_history"
DEFAULT_RSYNC_MIRROR = "rsync://ftpmirror.your.org/wikimedia-dumps/other/mediawiki_history"

# Current mediawiki_history dump files are headerless TSV. The public docs describe
# the fields, but the files include a couple of compatibility columns, so keep the
# exact indices we depend on instead of pretending the full schema is stable here.
WIKI_DB = 0
EVENT_ENTITY = 2
EVENT_TYPE = 3
EVENT_TIMESTAMP = 4
EVENT_COMMENT = 5
EVENT_USER_TEXT_HISTORICAL = 8
EVENT_USER_TEXT = 9
EVENT_USER_IS_BOT_BY_HISTORICAL = 14
EVENT_USER_IS_BOT_BY = 15
EVENT_USER_IS_BOT_COMPAT = 22
PAGE_ID = 28
PAGE_TITLE_HISTORICAL = 29
PAGE_TITLE = 30
PAGE_NAMESPACE_HISTORICAL = 31
REVISION_ID = 60
REVISION_TEXT_BYTES = 65
REVISION_TEXT_BYTES_DIFF = 66
REVISION_IS_IDENTITY_REVERTED = 72
REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID = 73
REVISION_IS_IDENTITY_REVERT = 75
HISTORICAL_BUCKET_MINUTES = 60
HISTORICAL_WINDOW_HOURS = 24
HISTORICAL_REVERT_EDGE_THRESHOLD = 4
HISTORICAL_ADDITIONAL_PAIR_WEIGHT = 0.25
ROUTINE_UPDATE_TITLE_RE = re.compile(
    r"\b("
    r"list of|season|championship|championships|tournament|cup|league|roster|squad|"
    r"standings|results|statistics|records|record|draft|episode|episodes|discography|"
    r"filmography|awards?|medal table|schedule|rankings?|transfers"
    r")\b",
    re.IGNORECASE,
)
STRONG_ROUTINE_UPDATE_TITLE_RE = re.compile(
    r"\b("
    r"list of (?:programs|episodes|missions|champions|winners|awards|records|songs|singles|"
    r"albums|characters|players|squads|rosters|results|standings|rankings)|"
    r"episodes?|discography|filmography|roster|squad|standings|results|schedule|"
    r"statistics|rankings?|transfers"
    r")\b",
    re.IGNORECASE,
)
SEASON_TITLE_RE = re.compile(
    r"\b(?:18|19|20)\d{2}(?:[-\u2013]\d{2})?\b.*\b(season|championship|tournament|cup|league|draft|election results)\b",
    re.IGNORECASE,
)
TALK_RFC_RE = re.compile(
    r"\b("
    r"rfc|rfc/u|request(?:s|ed)? for comment|requested comment|request for comments"
    r")\b",
    re.IGNORECASE,
)
TALK_ARBITRATION_RE = re.compile(
    r"\b("
    r"arbcom|arbitration|arbitration enforcement|wp:ae|ae noticeboard|"
    r"discretionary sanctions|contentious topic|contentious topics|arbcom case"
    r")\b",
    re.IGNORECASE,
)
TALK_RESTRICTION_RE = re.compile(
    r"\b("
    r"page protection|protected|semi-protected|fully protected|full protection|"
    r"extended[- ]confirmed|extendedconfirmed|ecp|pending changes|"
    r"edit restriction|editing restriction|topic ban|1rr|0rr|500/30|"
    r"move protected|protection request|requested protection"
    r")\b",
    re.IGNORECASE,
)


@dataclass
class HistoricalRevision:
    wiki: str
    page_id: int
    page_title: str
    timestamp: datetime
    user_text: str
    comment: str
    rev_id: int
    text_bytes: int
    text_bytes_diff: int
    is_identity_reverted: bool
    first_identity_reverting_revision_id: int | None
    is_identity_revert: bool


@dataclass
class HistoricalBucket:
    edit_count: int = 0
    editors: Counter[str] = field(default_factory=Counter)
    revert_edges: Counter[tuple[str, str]] = field(default_factory=Counter)

    def add_revision(self, revision: HistoricalRevision) -> None:
        self.edit_count += 1
        self.editors[revision.user_text] += 1

    def add_revert(self, reverter: str, reverted: str) -> None:
        if reverter and reverted and reverter != reverted:
            self.revert_edges[(reverter, reverted)] += 1


@dataclass
class PageHistoricalAggregate:
    wiki: str
    page_id: int
    page_title: str
    edit_count: int = 0
    editors: set[str] = field(default_factory=set)
    unique_editor_count: int | None = None
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None
    raw_reverted_count: int = 0
    raw_revert_count: int = 0
    text_bytes: int = 0
    talk_page_id: int | None = None
    talk_edit_count: int = 0
    talk_editors: set[str] = field(default_factory=set)
    talk_unique_editor_count: int | None = None
    talk_text_bytes: int = 0
    talk_rfc_count: int = 0
    talk_arbitration_count: int = 0
    talk_restriction_count: int = 0
    revert_edges: Counter[tuple[str, str]] = field(default_factory=Counter)
    buckets: dict[datetime, HistoricalBucket] = field(default_factory=dict)

    def add_revision(self, revision: HistoricalRevision) -> None:
        self.page_title = revision.page_title or self.page_title
        self.edit_count += 1
        self.editors.add(revision.user_text)
        if self.first_timestamp is None or revision.timestamp < self.first_timestamp:
            self.first_timestamp = revision.timestamp
        if self.last_timestamp is None or revision.timestamp > self.last_timestamp:
            self.last_timestamp = revision.timestamp
            self.text_bytes = max(0, revision.text_bytes)
        self.raw_reverted_count += int(revision.is_identity_reverted)
        self.raw_revert_count += int(revision.is_identity_revert)
        self._bucket(revision.timestamp).add_revision(revision)

    def add_revert(self, reverter: str, reverted: str, timestamp: datetime | None = None) -> None:
        if reverter and reverted and reverter != reverted:
            self.revert_edges[(reverter, reverted)] += 1
            if timestamp is not None:
                self._bucket(timestamp).add_revert(reverter, reverted)

    def _bucket(self, timestamp: datetime) -> HistoricalBucket:
        bucket_start = _bucket_start(timestamp)
        return self.buckets.setdefault(bucket_start, HistoricalBucket())

    def add_talk_revision(self, revision: HistoricalRevision) -> None:
        self.talk_page_id = revision.page_id
        self.talk_edit_count += 1
        self.talk_editors.add(revision.user_text)
        self.talk_unique_editor_count = None
        # Exclude identity-reverted revisions so a single vandalism/spam dump
        # (e.g. 4 MB of link spam that was reverted minutes later) does not
        # inflate the talk-size proxy used by the "most discussed" ranking.
        if not revision.is_identity_reverted:
            self.talk_text_bytes = max(self.talk_text_bytes, max(0, revision.text_bytes))
        rfc_count, arbitration_count, restriction_count = talk_governance_counts(revision.comment)
        self.talk_rfc_count += rfc_count
        self.talk_arbitration_count += arbitration_count
        self.talk_restriction_count += restriction_count


@dataclass(frozen=True)
class HistoricalJobResult:
    period: str
    rows_seen: int
    revisions_seen: int
    pages_scored: int
    rows_written: int


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_map = dict(attrs)
        href = attrs_map.get("href")
        if href:
            self.hrefs.append(href)


def dump_url(snapshot: str, wiki: str, partition: str) -> str:
    return f"{DUMPS_BASE_URL}/{snapshot}/{wiki}/{snapshot}.{wiki}.{partition}.tsv.bz2"


def rsync_source(snapshot: str, wiki: str, mirror: str = DEFAULT_RSYNC_MIRROR) -> str:
    return f"{mirror.rstrip('/')}/{snapshot}/{wiki}/"


def list_dump_partitions(snapshot: str, wiki: str) -> list[str]:
    url = f"{DUMPS_BASE_URL}/{snapshot}/{wiki}/"
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        html = response.text
    parser = LinkParser()
    parser.feed(html)
    pattern = re.compile(rf"^{re.escape(snapshot)}\.{re.escape(wiki)}\.(.+)\.tsv\.bz2$")
    partitions = []
    for href in parser.hrefs:
        match = pattern.match(href)
        if match:
            partitions.append(match.group(1))
    return sorted(partitions)


def download_dump(snapshot: str, wiki: str, partition: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{snapshot}.{wiki}.{partition}.tsv.bz2"
    target = output_dir / filename
    if target.exists():
        return target
    partial = target.with_suffix(target.suffix + ".part")
    if partial.exists():
        partial.unlink()
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=None) as client:
        with client.stream("GET", dump_url(snapshot, wiki, partition)) as response:
            response.raise_for_status()
            with partial.open("wb") as file:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    file.write(chunk)
    partial.replace(target)
    return target


def prefetch_history_dumps(
    *,
    snapshot: str,
    wiki: str = "enwiki",
    output_dir: Path = Path("data/dumps"),
    mirror: str = DEFAULT_RSYNC_MIRROR,
    start_partition: str | None = None,
    stop_partition: str | None = None,
    dry_run: bool = False,
) -> None:
    rsync = shutil.which("rsync")
    if not rsync:
        raise RuntimeError("rsync is not installed or not on PATH")
    output_dir.mkdir(parents=True, exist_ok=True)
    includes = ["--include", "*/"]
    if start_partition or stop_partition:
        partitions = list_dump_partitions(snapshot, wiki)
        if start_partition:
            partitions = [partition for partition in partitions if partition >= start_partition]
        if stop_partition:
            partitions = [partition for partition in partitions if partition <= stop_partition]
        for partition in partitions:
            includes.extend(["--include", f"{snapshot}.{wiki}.{partition}.tsv.bz2"])
    else:
        includes.extend(["--include", f"{snapshot}.{wiki}.*.tsv.bz2"])
    command = [
        rsync,
        "-av",
        "--partial",
        "--progress",
        "--size-only",
        *includes,
        "--exclude",
        "*",
        rsync_source(snapshot, wiki, mirror),
        f"{output_dir}/",
    ]
    log("prefetch_start " + " ".join(command))
    if dry_run:
        return
    subprocess.run(command, check=True)
    log(f"prefetch_done snapshot={snapshot} wiki={wiki} output_dir={output_dir}")


def backfill_snapshot(
    *,
    snapshot: str,
    wiki: str = "enwiki",
    output_dir: Path = Path("data/dumps"),
    period_prefix: str = "history",
    limit: int = 100,
    min_score: float = 40.0,
    namespace: int = 0,
    keep_downloads: bool = True,
    sleep_seconds: float = 2.0,
    start_partition: str | None = None,
    stop_partition: str | None = None,
    workers: int = 1,
) -> None:
    init_db()
    partitions = list_dump_partitions(snapshot, wiki)
    if start_partition:
        partitions = [partition for partition in partitions if partition >= start_partition]
    if stop_partition:
        partitions = [partition for partition in partitions if partition <= stop_partition]

    log(
        f"backfill_start snapshot={snapshot} wiki={wiki} partitions={len(partitions)} "
        f"limit={limit} min_score={min_score} keep_downloads={keep_downloads} workers={workers}"
    )
    pending = []
    for index, partition in enumerate(partitions, start=1):
        period = f"{period_prefix}:{snapshot}:{partition}"
        with SessionLocal() as session:
            if historical_processed_period_exists(session, period) or historical_aggregate_period_exists(session, period):
                log(f"skip_existing index={index}/{len(partitions)} period={period}")
                continue
        pending.append((index, partition, period))

    if workers <= 1:
        for index, partition, period in pending:
            run_backfill_partition(
                snapshot=snapshot,
                wiki=wiki,
                partition=partition,
                period=period,
                output_dir=output_dir,
                limit=limit,
                min_score=min_score,
                namespace=namespace,
                keep_downloads=keep_downloads,
                index=index,
                total=len(partitions),
                sleep_seconds=sleep_seconds,
            )
    else:
        run_parallel_backfill(
            pending=pending,
            snapshot=snapshot,
            wiki=wiki,
            output_dir=output_dir,
            limit=limit,
            min_score=min_score,
            namespace=namespace,
            keep_downloads=keep_downloads,
            workers=workers,
            total=len(partitions),
        )
    log(f"backfill_complete snapshot={snapshot} wiki={wiki}")


def run_parallel_backfill(
    *,
    pending: list[tuple[int, str, str]],
    snapshot: str,
    wiki: str,
    output_dir: Path,
    limit: int,
    min_score: float,
    namespace: int,
    keep_downloads: bool,
    workers: int,
    total: int,
) -> None:
    if not pending:
        return
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                run_backfill_partition,
                snapshot=snapshot,
                wiki=wiki,
                partition=partition,
                period=period,
                output_dir=output_dir,
                limit=limit,
                min_score=min_score,
                namespace=namespace,
                keep_downloads=keep_downloads,
                index=index,
                total=total,
                sleep_seconds=0,
            ): period
            for index, partition, period in pending
        }
        for future in as_completed(futures):
            period = futures[future]
            try:
                future.result()
            except Exception as exc:
                log(f"error period={period} type={type(exc).__name__} message={exc}")
                raise


def run_backfill_partition(
    *,
    snapshot: str,
    wiki: str,
    partition: str,
    period: str,
    output_dir: Path,
    limit: int,
    min_score: float,
    namespace: int,
    keep_downloads: bool,
    index: int,
    total: int,
    sleep_seconds: float,
) -> HistoricalJobResult:
    with SessionLocal() as session:
        if historical_processed_period_exists(session, period) or historical_aggregate_period_exists(session, period):
            log(f"skip_existing index={index}/{total} period={period}")
            return HistoricalJobResult(period=period, rows_seen=0, revisions_seen=0, pages_scored=0, rows_written=0)

    path: Path | None = None
    started = time.monotonic()
    try:
        log(f"download_start index={index}/{total} period={period}")
        path = download_dump(snapshot, wiki, partition, output_dir)
        size_mb = path.stat().st_size / 1024 / 1024
        log(f"download_done period={period} path={path} size_mb={size_mb:.2f}")
        result = process_history_files(
            [path],
            period=period,
            limit=limit,
            min_score=min_score,
            namespace=namespace,
            write=True,
        )
        elapsed = time.monotonic() - started
        log(
            f"process_done period={period} rows_seen={result.rows_seen} "
            f"revisions_seen={result.revisions_seen} pages_scored={result.pages_scored} "
            f"rows_written={result.rows_written} elapsed_seconds={elapsed:.1f}"
        )
        return result
    except Exception as exc:
        log(f"error period={period} type={type(exc).__name__} message={exc}")
        raise
    finally:
        if path and path.exists() and not keep_downloads:
            path.unlink()
            log(f"download_removed period={period} path={path}")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def iter_historical_revisions(path: Path, namespace: int = 0) -> Iterable[HistoricalRevision]:
    with bz2.open(path, "rt", encoding="utf-8", newline="") as file:
        for line in file:
            revision = parse_history_row(line.rstrip("\n").split("\t"), namespace)
            if revision:
                yield revision


def parse_history_row(columns: list[str], namespace: int = 0) -> HistoricalRevision | None:
    if len(columns) < 78:
        return None
    if columns[EVENT_ENTITY] != "revision" or columns[EVENT_TYPE] != "create":
        return None
    if _int(columns[PAGE_NAMESPACE_HISTORICAL]) != namespace:
        return None
    if _is_bot(columns):
        return None

    page_id = _int(columns[PAGE_ID])
    rev_id = _int(columns[REVISION_ID])
    if not page_id or not rev_id:
        return None

    user_text = columns[EVENT_USER_TEXT] or columns[EVENT_USER_TEXT_HISTORICAL] or "unknown"
    timestamp = _timestamp(columns[EVENT_TIMESTAMP])
    if timestamp is None:
        return None

    return HistoricalRevision(
        wiki=columns[WIKI_DB],
        page_id=page_id,
        page_title=columns[PAGE_TITLE] or columns[PAGE_TITLE_HISTORICAL] or f"page:{page_id}",
        timestamp=timestamp,
        user_text=user_text,
        comment=columns[EVENT_COMMENT],
        rev_id=rev_id,
        text_bytes=_int(columns[REVISION_TEXT_BYTES]) or 0,
        text_bytes_diff=_int(columns[REVISION_TEXT_BYTES_DIFF]) or 0,
        is_identity_reverted=_bool(columns[REVISION_IS_IDENTITY_REVERTED]),
        first_identity_reverting_revision_id=_int(
            columns[REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID]
        ),
        is_identity_revert=_bool(columns[REVISION_IS_IDENTITY_REVERT]),
    )


def talk_governance_counts(comment: str) -> tuple[int, int, int]:
    if not substantive_talk_comment(comment):
        return (0, 0, 0)
    return (
        int(bool(TALK_RFC_RE.search(comment))),
        int(bool(TALK_ARBITRATION_RE.search(comment))),
        int(bool(TALK_RESTRICTION_RE.search(comment))),
    )


def substantive_talk_comment(comment: str) -> bool:
    normalized = comment.strip()
    if len(normalized) < 3:
        return False
    lower = normalized.lower()
    return not (
        lower.startswith("reverted edits by")
        or lower.startswith("undid revision")
        or lower.startswith("rollback")
        or lower.startswith("automated")
    )


def process_history_files(
    paths: list[Path],
    *,
    period: str,
    limit: int = 100,
    min_score: float = 40.0,
    namespace: int = 0,
    write: bool = True,
) -> HistoricalJobResult:
    aggregates: dict[tuple[str, int], PageHistoricalAggregate] = {}
    aggregates_by_title: dict[tuple[str, str], PageHistoricalAggregate] = {}
    pending_talk_revisions: dict[tuple[str, str], list[HistoricalRevision]] = defaultdict(list)
    pending_reverts: dict[int, list[HistoricalRevision]] = defaultdict(list)
    rows_seen = 0
    revisions_seen = 0

    for path in paths:
        with bz2.open(path, "rt", encoding="utf-8", newline="") as file:
            for line in file:
                rows_seen += 1
                columns = line.rstrip("\n").split("\t")
                row_namespace = _int(columns[PAGE_NAMESPACE_HISTORICAL]) if len(columns) > PAGE_NAMESPACE_HISTORICAL else -1
                if namespace == 0 and row_namespace not in {0, 1}:
                    continue
                if namespace != 0 and row_namespace != namespace:
                    continue
                revision = parse_history_row(columns, row_namespace)
                if revision is None:
                    continue
                revisions_seen += 1
                if row_namespace == 1:
                    title_key = (revision.wiki, normalize_history_title(revision.page_title))
                    article = aggregates_by_title.get(title_key)
                    if article:
                        article.add_talk_revision(revision)
                    else:
                        pending_talk_revisions[title_key].append(revision)
                    continue

                key = (revision.wiki, revision.page_id)
                aggregate = aggregates.setdefault(
                    key,
                    PageHistoricalAggregate(
                        wiki=revision.wiki,
                        page_id=revision.page_id,
                        page_title=revision.page_title,
                    ),
                )
                aggregates_by_title[(revision.wiki, normalize_history_title(revision.page_title))] = aggregate
                aggregate.add_revision(revision)
                title_key = (revision.wiki, normalize_history_title(revision.page_title))
                for talk_revision in pending_talk_revisions.pop(title_key, []):
                    aggregate.add_talk_revision(talk_revision)

                reverted_user = dominant_reverted_user(
                    pending_reverts.pop(revision.rev_id, []),
                    revision.page_id,
                )
                if reverted_user:
                    aggregate.add_revert(revision.user_text, reverted_user, revision.timestamp)

                if revision.is_identity_reverted and revision.first_identity_reverting_revision_id:
                    pending_reverts[revision.first_identity_reverting_revision_id].append(revision)

    scored_rows = [
        row
        for row in (score_historical_page(aggregate) for aggregate in aggregates.values())
        if candidate_row_passes_min_score(row, min_score)
    ]
    selected = rank_historical_rows(scored_rows, limit)

    if write:
        init_db()
        with SessionLocal() as session:
            aggregate_records = [
                record
                for aggregate in aggregates.values()
                if (record := aggregate_to_record(aggregate)) is not None
            ]
            replace_historical_aggregates(
                session,
                period,
                aggregate_records,
            )
            replace_scoreboard_snapshot(session, period, selected)
            mark_historical_period_processed(
                session,
                period=period,
                rows_seen=rows_seen,
                revisions_seen=revisions_seen,
                pages_scored=len(aggregates),
                aggregate_count=len(aggregate_records),
            )

    return HistoricalJobResult(
        period=period,
        rows_seen=rows_seen,
        revisions_seen=revisions_seen,
        pages_scored=len(aggregates),
        rows_written=len(selected) if write else 0,
    )


def recompute_scoreboard_from_aggregates(
    *,
    period: str | None = None,
    limit: int = 100,
    min_score: float = 40.0,
) -> list[str]:
    init_db()
    with SessionLocal() as session:
        periods = [period] if period else historical_aggregate_periods(session)
        rewritten: list[str] = []
        for selected_period in periods:
            aggregates = [record_to_aggregate(record) for record in load_historical_aggregates(session, selected_period)]
            scored_rows = [
                row
                for row in (score_historical_page(aggregate) for aggregate in aggregates)
                if candidate_row_passes_min_score(row, min_score)
            ]
            selected = rank_historical_rows(scored_rows, limit)
            replace_scoreboard_snapshot(session, selected_period, selected)
            rewritten.append(selected_period)
    return rewritten


def historical_year_scoreboard(
    session,
    *,
    period: str,
    limit: int = 100,
    min_score: float = 40.0,
    method: str = "edit-war",
) -> list[dict[str, object]]:
    if method == "page-war":
        return historical_year_page_war_scoreboard(session, period=period, limit=limit, min_score=min_score)
    if method == "most-discussed":
        return historical_year_most_discussed_scoreboard(session, period=period, limit=limit)
    if method == "governance":
        return historical_year_governance_scoreboard(session, period=period, limit=limit, min_score=min_score)

    candidate_limit = max(limit * 10, 1000)
    aggregates = [
        record_to_aggregate(record)
        for record in load_historical_year_aggregates(session, period, candidate_limit)
    ]
    scored_rows = [
        row
        for row in (score_historical_page(aggregate) for aggregate in aggregates)
        if candidate_row_passes_min_score(row, min_score)
        and not probable_revert_only_cleanup_row(row)
    ]
    selected = rank_historical_rows(scored_rows, limit)
    for rank, row in enumerate(selected, start=1):
        row["id"] = None
        row["period"] = period
        row["rank"] = rank
        row["ranking_method"] = "edit-war"
    return selected


def historical_year_page_war_scoreboard(
    session,
    *,
    period: str,
    limit: int = 100,
    min_score: float = 40.0,
) -> list[dict[str, object]]:
    candidate_limit = max(limit * 20, 1000)
    rows = [
        page_war_score_row(record)
        for record in load_historical_year_page_stats(session, period, candidate_limit, order="page-war")
    ]
    rows = [
        row
        for row in rows
        if float(row["candidate_score"]) >= min_score
        and not probable_routine_page_war_row(row)
    ]
    rows.sort(
        key=lambda row: (
            row["candidate_score"],
            row["raw_reverted_count"] + row["raw_revert_count"],
            row["talk_text_bytes"],
            row["edit_count"],
        ),
        reverse=True,
    )
    selected = rows[:limit]
    for rank, row in enumerate(selected, start=1):
        row["id"] = None
        row["period"] = period
        row["rank"] = rank
        row["ranking_method"] = "page-war"
    return selected


def historical_year_most_discussed_scoreboard(
    session,
    *,
    period: str,
    limit: int = 100,
) -> list[dict[str, object]]:
    candidate_limit = max(limit * 20, 1000)
    rows = [
        most_discussed_score_row(record)
        for record in load_historical_year_page_stats(session, period, candidate_limit, order="most-discussed")
    ]
    rows = [row for row in rows if int(row["talk_text_bytes"]) > 0 or int(row["talk_edit_count"]) > 0]
    rows.sort(
        key=lambda row: (
            row["candidate_score"],
            row["talk_edit_count"],
            row["raw_reverted_count"] + row["raw_revert_count"],
        ),
        reverse=True,
    )
    selected = rows[:limit]
    for rank, row in enumerate(selected, start=1):
        row["id"] = None
        row["period"] = period
        row["rank"] = rank
        row["ranking_method"] = "most-discussed"
    return selected


def historical_year_governance_scoreboard(
    session,
    *,
    period: str,
    limit: int = 100,
    min_score: float = 40.0,
) -> list[dict[str, object]]:
    candidate_limit = max(limit * 100, 5000)
    records = load_historical_year_page_stats(session, period, candidate_limit, order="governance")
    admin_signals = load_page_admin_signals(
        session,
        [(str(record["wiki"]), int(record["page_id"])) for record in records],
    )
    title_signals = load_page_admin_title_signals(
        session,
        [
            (str(record["wiki"]), normalize_history_title(str(record["page_title"])))
            for record in records
        ],
    )
    rows = [
        governance_score_row(
            record,
            admin_signals.get((str(record["wiki"]), int(record["page_id"]))),
            title_signals.get((str(record["wiki"]), normalize_history_title(str(record["page_title"])))),
        )
        for record in records
    ]
    rows = [
        row
        for row in rows
        if float(row["candidate_score"]) >= min_score
        and int(row["governance_evidence_count"]) > 0
    ]
    rows.sort(
        key=lambda row: (
            row["candidate_score"],
            row["governance_evidence_count"],
            row["talk_text_bytes"],
            row["raw_reverted_count"] + row["raw_revert_count"],
        ),
        reverse=True,
    )
    selected = rows[:limit]
    for rank, row in enumerate(selected, start=1):
        row["id"] = None
        row["period"] = period
        row["rank"] = rank
        row["ranking_method"] = "governance"
    return selected


def page_war_score_row(record: dict[str, object]) -> dict[str, object]:
    edit_count = int(record.get("edit_count") or 0)
    unique_editors = int(record.get("unique_editors") or 0)
    raw_reverted_count = int(record.get("raw_reverted_count") or 0)
    raw_revert_count = int(record.get("raw_revert_count") or 0)
    mutual_pairs = int(record.get("mutual_revert_pairs") or 0)
    mutual_reverts = int(record.get("revert_count") or 0)
    talk_edit_count = int(record.get("talk_edit_count") or 0)
    talk_text_bytes = int(record.get("talk_text_bytes") or 0)
    raw_revert_activity = raw_reverted_count + raw_revert_count
    score = (
        math.sqrt(raw_reverted_count) * 24.0
        + math.sqrt(raw_revert_count) * 18.0
        + math.sqrt(max(0, mutual_reverts)) * 16.0
        + min(42.0, mutual_pairs * 8.0)
        + min(50.0, math.log1p(max(0, edit_count)) * 6.0)
        + min(35.0, math.sqrt(max(0, unique_editors)) * 4.2)
        + min(45.0, math.log1p(max(0, talk_text_bytes)) * 3.2)
        + min(25.0, math.sqrt(max(0, talk_edit_count)) * 4.0)
    )
    row = {
        **record,
        "peak_score": round(score, 2),
        "candidate_score": round(score * (1.0 - routine_update_penalty({**record, "peak_score": score})), 2),
        "score_area": 0.0,
        "war_minutes": 0,
        "episode_count": 0,
        "raw_reverted_count": raw_reverted_count,
        "raw_revert_count": raw_revert_count,
        "revert_count": mutual_reverts,
        "mutual_revert_pairs": mutual_pairs,
        "raw_revert_activity": raw_revert_activity,
        "talk_edit_count": talk_edit_count,
        "talk_text_bytes": talk_text_bytes,
        "battle_count": None,
        "talk_evidence_count": talk_edit_count,
    }
    row["routine_penalty"] = round(routine_update_penalty(row), 4)
    row["candidate_score"] = round(score * (1.0 - float(row["routine_penalty"])), 2)
    return row


def governance_score_row(
    record: dict[str, object],
    admin_signal: dict[str, object] | None = None,
    title_signal: dict[str, object] | None = None,
) -> dict[str, object]:
    admin_signal = admin_signal or {}
    title_signal = title_signal or {}
    raw_reverted_count = int(record.get("raw_reverted_count") or 0)
    raw_revert_count = int(record.get("raw_revert_count") or 0)
    mutual_reverts = int(record.get("revert_count") or 0)
    mutual_pairs = int(record.get("mutual_revert_pairs") or 0)
    talk_edit_count = int(record.get("talk_edit_count") or 0)
    talk_unique_editors = int(record.get("talk_unique_editors") or 0)
    talk_text_bytes = int(record.get("talk_text_bytes") or 0)
    talk_rfc_count = int(record.get("talk_rfc_count") or 0)
    talk_arbitration_count = int(record.get("talk_arbitration_count") or 0)
    talk_restriction_count = int(record.get("talk_restriction_count") or 0)
    restriction_count = int(admin_signal.get("restriction_count") or 0)
    protection_event_count = (
        int(admin_signal.get("protection_event_count") or 0)
        + int(title_signal.get("protection_event_count") or 0)
    )
    protection_days = (
        float(admin_signal.get("protection_days") or 0.0)
        + float(title_signal.get("protection_days") or 0.0)
    )
    has_extendedconfirmed = bool(admin_signal.get("has_extendedconfirmed"))
    has_sysop = bool(admin_signal.get("has_sysop"))
    has_cascade = bool(admin_signal.get("has_cascade"))

    debate_score = (
        talk_rfc_count * 90.0
        + talk_arbitration_count * 120.0
        + talk_restriction_count * 70.0
        + min(110.0, math.sqrt(max(0, talk_edit_count)) * 10.0)
        + min(95.0, math.log1p(max(0, talk_text_bytes)) * 6.0)
        + min(45.0, math.sqrt(max(0, talk_unique_editors)) * 4.0)
    )
    protection_score = (
        restriction_count * 70.0
        + (130.0 if has_sysop else 0.0)
        + (100.0 if has_extendedconfirmed else 0.0)
        + (35.0 if has_cascade else 0.0)
        + min(180.0, protection_event_count * 18.0)
        + min(160.0, math.sqrt(max(0.0, protection_days)) * 9.0)
    )
    article_score = (
        min(180.0, math.sqrt(max(0, raw_reverted_count + raw_revert_count)) * 17.0)
        + min(90.0, math.sqrt(max(0, mutual_reverts)) * 11.0)
        + min(80.0, mutual_pairs * 12.0)
    )
    governance_evidence_count = (
        talk_rfc_count
        + talk_arbitration_count
        + talk_restriction_count
        + restriction_count
        + protection_event_count
    )
    score = debate_score + protection_score + article_score
    row = {
        **record,
        "peak_score": round(score, 2),
        "candidate_score": round(score, 2),
        "score_area": 0.0,
        "war_minutes": 0,
        "episode_count": 0,
        "raw_reverted_count": raw_reverted_count,
        "raw_revert_count": raw_revert_count,
        "revert_count": mutual_reverts,
        "mutual_revert_pairs": mutual_pairs,
        "talk_edit_count": talk_edit_count,
        "talk_unique_editors": talk_unique_editors,
        "talk_text_bytes": talk_text_bytes,
        "talk_rfc_count": talk_rfc_count,
        "talk_arbitration_count": talk_arbitration_count,
        "talk_restriction_count": talk_restriction_count,
        "restriction_count": restriction_count,
        "restriction_types": str(admin_signal.get("restriction_types") or ""),
        "restriction_levels": str(admin_signal.get("restriction_levels") or ""),
        "restriction_expiry": str(admin_signal.get("restriction_expiry") or ""),
        "has_extendedconfirmed": has_extendedconfirmed,
        "has_sysop": has_sysop,
        "has_cascade": has_cascade,
        "protection_event_count": protection_event_count,
        "protection_days": round(protection_days, 2),
        "first_protection_at": title_signal.get("first_protection_at"),
        "last_protection_at": title_signal.get("last_protection_at"),
        "governance_evidence_count": governance_evidence_count,
        "battle_count": None,
        "talk_evidence_count": talk_rfc_count + talk_arbitration_count + talk_restriction_count,
    }
    routine_penalty = routine_update_penalty(row)
    if governance_evidence_count:
        routine_penalty *= 0.35
    row["routine_penalty"] = round(routine_penalty, 4)
    row["candidate_score"] = round(score * (1.0 - routine_penalty), 2)
    return row


def most_discussed_score_row(record: dict[str, object]) -> dict[str, object]:
    talk_text_bytes = int(record.get("talk_text_bytes") or 0)
    talk_edit_count = int(record.get("talk_edit_count") or 0)
    score = float(talk_text_bytes)
    return {
        **record,
        "peak_score": score,
        "candidate_score": score,
        "score_area": 0.0,
        "war_minutes": 0,
        "episode_count": 0,
        "raw_reverted_count": int(record.get("raw_reverted_count") or 0),
        "raw_revert_count": int(record.get("raw_revert_count") or 0),
        "revert_count": int(record.get("revert_count") or 0),
        "mutual_revert_pairs": int(record.get("mutual_revert_pairs") or 0),
        "talk_edit_count": talk_edit_count,
        "talk_text_bytes": talk_text_bytes,
        "battle_count": None,
        "talk_evidence_count": talk_edit_count,
    }


def probable_routine_page_war_row(row: dict[str, object]) -> bool:
    title = str(row.get("page_title") or "")
    raw_revert_activity = int(row.get("raw_reverted_count") or 0) + int(row.get("raw_revert_count") or 0)
    talk_text_bytes = int(row.get("talk_text_bytes") or 0)
    if STRONG_ROUTINE_UPDATE_TITLE_RE.search(title.replace("_", " ")) and talk_text_bytes < 10_000:
        return raw_revert_activity < 45
    return False


def probable_revert_only_cleanup_row(row: dict[str, object]) -> bool:
    revert_density = float(row.get("revert_density") or 0.0)
    unique_editors = int(row.get("unique_editors") or 0)
    war_minutes = int(row.get("war_minutes") or 0)
    episode_count = int(row.get("episode_count") or 0)
    mutual_pairs = int(row.get("mutual_revert_pairs") or 0)
    short_cleanup_burst = (
        revert_density >= 0.82
        and unique_editors <= 16
        and war_minutes <= HISTORICAL_BUCKET_MINUTES * 2
        and episode_count <= 1
        and mutual_pairs <= 8
    )
    revert_only_churn = revert_density >= 0.82 and unique_editors <= 40 and mutual_pairs <= 8
    return short_cleanup_burst or revert_only_churn


def rank_historical_rows(rows: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    rows.sort(
        key=lambda row: (
            row.get("candidate_score", row["peak_score"]),
            row["mutual_revert_pairs"],
            row["revert_count"],
            row["peak_score"],
        ),
        reverse=True,
    )
    return rows[:limit]


def candidate_row_passes_min_score(row: dict[str, object], min_score: float) -> bool:
    return float(row.get("candidate_score", row.get("peak_score", 0.0)) or 0.0) >= min_score


def normalize_history_title(title: str) -> str:
    return title.replace(" ", "_").removeprefix("Talk:")


def aggregate_to_record(aggregate: PageHistoricalAggregate) -> dict[str, object] | None:
    revert_count = sum(mutual_revert_edges(effective_revert_edges(aggregate.revert_edges)).values())
    mutual_pairs, _ = mutual_revert_metrics_from_edges(effective_revert_edges(aggregate.revert_edges))
    unique_editors = aggregate.unique_editor_count if aggregate.unique_editor_count is not None else len(aggregate.editors)
    if not should_store_historical_aggregate(
        edit_count=aggregate.edit_count,
        unique_editors=unique_editors,
        raw_reverted_count=aggregate.raw_reverted_count,
        raw_revert_count=aggregate.raw_revert_count,
        mutual_revert_count=revert_count,
        mutual_revert_pairs=mutual_pairs,
        talk_edit_count=aggregate.talk_edit_count,
        talk_text_bytes=aggregate.talk_text_bytes,
        talk_rfc_count=aggregate.talk_rfc_count,
        talk_arbitration_count=aggregate.talk_arbitration_count,
        talk_restriction_count=aggregate.talk_restriction_count,
    ):
        return None
    return {
        "wiki": aggregate.wiki,
        "page_id": aggregate.page_id,
        "page_title": aggregate.page_title,
        "edit_count": aggregate.edit_count,
        "unique_editors": unique_editors,
        "revert_count": revert_count,
        "mutual_revert_pairs": mutual_pairs,
        "raw_reverted_count": aggregate.raw_reverted_count,
        "raw_revert_count": aggregate.raw_revert_count,
        "talk_page_id": aggregate.talk_page_id,
        "talk_edit_count": aggregate.talk_edit_count,
        "talk_unique_editors": len(aggregate.talk_editors),
        "talk_text_bytes": aggregate.talk_text_bytes,
        "talk_rfc_count": aggregate.talk_rfc_count,
        "talk_arbitration_count": aggregate.talk_arbitration_count,
        "talk_restriction_count": aggregate.talk_restriction_count,
        "first_timestamp": aggregate.first_timestamp,
        "last_timestamp": aggregate.last_timestamp,
        "buckets": [
            {
                "bucket_start": bucket_start,
                "edit_count": bucket.edit_count,
                "editors": dict(bucket.editors),
                "revert_edges": [
                    [reverter, reverted, count]
                    for (reverter, reverted), count in bucket.revert_edges.items()
                ],
            }
            for bucket_start, bucket in sorted(aggregate.buckets.items())
        ],
    }


def should_store_historical_aggregate(
    *,
    edit_count: int,
    unique_editors: int,
    raw_reverted_count: int,
    raw_revert_count: int,
    mutual_revert_count: int,
    mutual_revert_pairs: int,
    talk_edit_count: int,
    talk_text_bytes: int,
    talk_rfc_count: int = 0,
    talk_arbitration_count: int = 0,
    talk_restriction_count: int = 0,
) -> bool:
    raw_revert_activity = raw_reverted_count + raw_revert_count
    governance_mentions = talk_rfc_count + talk_arbitration_count + talk_restriction_count
    if mutual_revert_count > 0 or mutual_revert_pairs > 0:
        return True
    if raw_reverted_count >= 3 or raw_revert_count >= 3:
        return True
    if edit_count >= 40 and unique_editors >= 8 and raw_revert_activity >= 2:
        return True
    if talk_edit_count >= 3 or talk_text_bytes >= 12_000:
        return True
    if governance_mentions > 0:
        return True
    return False


def record_to_aggregate(record: dict[str, object]) -> PageHistoricalAggregate:
    aggregate = PageHistoricalAggregate(
        wiki=str(record["wiki"]),
        page_id=int(record["page_id"]),
        page_title=str(record["page_title"]),
        edit_count=int(record["edit_count"]),
        unique_editor_count=int(record["unique_editors"]),
        first_timestamp=record.get("first_timestamp"),  # type: ignore[arg-type]
        last_timestamp=record.get("last_timestamp"),  # type: ignore[arg-type]
        raw_reverted_count=int(record.get("raw_reverted_count") or 0),
        raw_revert_count=int(record.get("raw_revert_count") or 0),
        talk_page_id=int(record["talk_page_id"]) if record.get("talk_page_id") else None,
        talk_edit_count=int(record.get("talk_edit_count") or 0),
        talk_unique_editor_count=int(record.get("talk_unique_editors") or 0),
        talk_text_bytes=int(record.get("talk_text_bytes") or 0),
        talk_rfc_count=int(record.get("talk_rfc_count") or 0),
        talk_arbitration_count=int(record.get("talk_arbitration_count") or 0),
        talk_restriction_count=int(record.get("talk_restriction_count") or 0),
    )
    for bucket_record in record.get("buckets", []):  # type: ignore[union-attr]
        bucket = HistoricalBucket(
            edit_count=int(bucket_record["edit_count"]),
            editors=Counter({str(editor): int(count) for editor, count in bucket_record["editors"].items()}),
            revert_edges=Counter(
                {
                    (str(reverter), str(reverted)): int(count)
                    for reverter, reverted, count in bucket_record["revert_edges"]
                }
            ),
        )
        aggregate.buckets[bucket_record["bucket_start"]] = bucket
        aggregate.revert_edges.update(bucket.revert_edges)
    return aggregate


def dominant_reverted_user(revisions: list[HistoricalRevision], page_id: int) -> str | None:
    users = Counter(
        revision.user_text
        for revision in revisions
        if revision.page_id == page_id and revision.user_text
    )
    if not users:
        return None
    return users.most_common(1)[0][0]


def effective_revert_edges(edges: Counter[tuple[str, str]]) -> Counter[tuple[str, str]]:
    return Counter(
        {
            edge: count
            for edge, count in edges.items()
            if count >= HISTORICAL_REVERT_EDGE_THRESHOLD
        }
    )


def current_effective_revert_count(
    current_edges: Counter[tuple[str, str]],
    window_edges: Counter[tuple[str, str]],
) -> int:
    return sum(
        count
        for edge, count in current_edges.items()
        if window_edges[edge] >= HISTORICAL_REVERT_EDGE_THRESHOLD
    )


def mutual_revert_edges(edges: Counter[tuple[str, str]]) -> Counter[tuple[str, str]]:
    return Counter(
        {
            edge: count
            for edge, count in edges.items()
            if edges.get((edge[1], edge[0]), 0) > 0
        }
    )


def score_historical_page(aggregate: PageHistoricalAggregate) -> dict[str, object]:
    temporal = score_historical_page_windows(aggregate)
    effective_edges = effective_revert_edges(aggregate.revert_edges)
    contested_edges = mutual_revert_edges(effective_edges)
    revert_count = sum(contested_edges.values())
    unique_editors = aggregate.unique_editor_count if aggregate.unique_editor_count is not None else len(aggregate.editors)
    revert_density = revert_count / aggregate.edit_count if aggregate.edit_count else 0.0
    mutual_pairs, mutual_count = mutual_revert_metrics_from_edges(effective_edges)
    top_reverter_share = top_reverter_share_from_edges(contested_edges)
    strongest_pair_share = strongest_bidirectional_pair_share(contested_edges)
    row: dict[str, object] = {
        "wiki": aggregate.wiki,
        "page_id": aggregate.page_id,
        "page_title": aggregate.page_title,
        "score_area": temporal["score_area"],
        "peak_score": temporal["peak_score"],
        "war_minutes": temporal["war_minutes"],
        "episode_count": temporal["episode_count"],
        "edit_count": aggregate.edit_count,
        "unique_editors": unique_editors,
        "revert_count": revert_count,
        "mutual_revert_pairs": mutual_pairs,
        "mutual_revert_count": mutual_count,
        "raw_reverted_count": aggregate.raw_reverted_count,
        "raw_revert_count": aggregate.raw_revert_count,
        "talk_page_id": aggregate.talk_page_id,
        "talk_edit_count": aggregate.talk_edit_count,
        "talk_unique_editors": (
            aggregate.talk_unique_editor_count
            if aggregate.talk_unique_editor_count is not None
            else len(aggregate.talk_editors)
        ),
        "talk_text_bytes": aggregate.talk_text_bytes,
        "talk_rfc_count": aggregate.talk_rfc_count,
        "talk_arbitration_count": aggregate.talk_arbitration_count,
        "talk_restriction_count": aggregate.talk_restriction_count,
        "revert_density": round(revert_density, 4),
        "top_reverter_share": round(top_reverter_share, 4),
        "strongest_pair_share": round(strongest_pair_share, 4),
    }
    penalty = routine_update_penalty(row)
    row["routine_penalty"] = round(penalty, 4)
    row["candidate_score"] = round(float(row["peak_score"]) * (1.0 - penalty), 2)
    return row


def score_historical_page_windows(aggregate: PageHistoricalAggregate) -> dict[str, float | int]:
    bucket_items = sorted(aggregate.buckets.items())
    if not bucket_items:
        return {"peak_score": 0.0, "score_area": 0.0, "war_minutes": 0, "episode_count": 0}

    window_buckets: list[tuple[datetime, HistoricalBucket]] = []
    window_edit_count = 0
    window_editors: Counter[str] = Counter()
    window_revert_edges: Counter[tuple[str, str]] = Counter()
    peak_score = 0.0
    war_minutes = 0
    episode_count = 0
    in_episode = False
    previous_bucket_start: datetime | None = None

    for bucket_start, bucket in bucket_items:
        if previous_bucket_start and bucket_start - previous_bucket_start > timedelta(hours=HISTORICAL_WINDOW_HOURS):
            in_episode = False
        previous_bucket_start = bucket_start

        window_buckets.append((bucket_start, bucket))
        window_edit_count += bucket.edit_count
        window_editors.update(bucket.editors)
        window_revert_edges.update(bucket.revert_edges)

        cutoff = bucket_start - timedelta(hours=HISTORICAL_WINDOW_HOURS - 1)
        while window_buckets and window_buckets[0][0] < cutoff:
            _, expired = window_buckets.pop(0)
            window_edit_count -= expired.edit_count
            _subtract_counter(window_editors, expired.editors)
            _subtract_counter(window_revert_edges, expired.revert_edges)

        effective_window_edges = effective_revert_edges(window_revert_edges)
        mutual_window_edges = mutual_revert_edges(effective_window_edges)
        current_reverts = current_effective_revert_count(bucket.revert_edges, mutual_window_edges)
        score = _historical_window_score(
            edit_count=window_edit_count,
            unique_editors=sum(1 for count in window_editors.values() if count > 0),
            revert_edges=mutual_window_edges,
            current_reverts=current_reverts,
        )
        peak_score = max(peak_score, score)
        if score >= 60 and current_reverts > 0:
            war_minutes += HISTORICAL_BUCKET_MINUTES
            if not in_episode:
                episode_count += 1
            in_episode = True
        elif score < 40 or current_reverts == 0:
            in_episode = False

    return {
        "peak_score": round(peak_score, 2),
        "score_area": 0.0,
        "war_minutes": war_minutes,
        "episode_count": episode_count,
    }


def _historical_window_score(
    *,
    edit_count: int,
    unique_editors: int,
    revert_edges: Counter[tuple[str, str]],
    current_reverts: int,
) -> float:
    revert_count = sum(revert_edges.values())
    if unique_editors < 2 or revert_count == 0:
        return 0.0

    reverter_counts: Counter[str] = Counter()
    for (reverter, _), count in revert_edges.items():
        reverter_counts[reverter] += count
    top_reverter_share = max(reverter_counts.values()) / revert_count if reverter_counts else 0.0
    mutual_pairs, mutual_count = mutual_revert_metrics_from_edges(revert_edges)
    if mutual_pairs == 0 or mutual_count == 0:
        return 0.0

    pair_strengths, revert_participants = bidirectional_pair_strengths(revert_edges)
    weighted_pair_points = sum(
        strength * (1.0 if index == 0 else HISTORICAL_ADDITIONAL_PAIR_WEIGHT)
        for index, strength in enumerate(pair_strengths)
    )
    contested_density = revert_count / edit_count if edit_count else 0.0
    concentration_multiplier = 0.25 + min(0.75, contested_density * 0.9)
    combatant_points = min(12.0, max(0, len(revert_participants) - 2) * 4.0)
    recency_points = min(4.0, current_reverts)
    cleanup_multiplier = 0.55 if top_reverter_share >= 0.75 and revert_count >= 8 else 1.0
    score = (
        (weighted_pair_points * concentration_multiplier)
        + combatant_points
        + recency_points
    ) * cleanup_multiplier
    return max(0.0, score)


def bidirectional_pair_strengths(edges: Counter[tuple[str, str]]) -> tuple[list[float], set[str]]:
    seen: set[frozenset[str]] = set()
    strengths: list[float] = []
    participants: set[str] = set()
    for (left, right), left_count in edges.items():
        pair_key = frozenset({left, right})
        if pair_key in seen:
            continue
        right_count = edges.get((right, left), 0)
        if not right_count:
            continue
        seen.add(pair_key)
        participants.update((left, right))
        balanced_reverts = min(left_count, right_count)
        stronger_side_reverts = max(left_count, right_count)
        strengths.append((math.sqrt(balanced_reverts) * 42.0) + (stronger_side_reverts * 2.0))
    strengths.sort(reverse=True)
    return strengths, participants


def top_reverter_share_from_edges(edges: Counter[tuple[str, str]]) -> float:
    revert_count = sum(edges.values())
    if revert_count == 0:
        return 0.0
    reverter_counts: Counter[str] = Counter()
    for (reverter, _), count in edges.items():
        reverter_counts[reverter] += count
    return max(reverter_counts.values()) / revert_count if reverter_counts else 0.0


def strongest_bidirectional_pair_share(edges: Counter[tuple[str, str]]) -> float:
    revert_count = sum(edges.values())
    if revert_count == 0:
        return 0.0
    seen: set[frozenset[str]] = set()
    strongest = 0
    for (left, right), left_count in edges.items():
        pair_key = frozenset({left, right})
        if pair_key in seen:
            continue
        right_count = edges.get((right, left), 0)
        if not right_count:
            continue
        seen.add(pair_key)
        strongest = max(strongest, left_count + right_count)
    return strongest / revert_count if strongest else 0.0


def routine_update_penalty(row: dict[str, object]) -> float:
    title = str(row.get("page_title") or "").replace("_", " ")
    peak_score = float(row.get("peak_score") or 0.0)
    edit_count = int(row.get("edit_count") or 0)
    unique_editors = int(row.get("unique_editors") or 0)
    revert_count = int(row.get("revert_count") or 0)
    mutual_pairs = int(row.get("mutual_revert_pairs") or 0)
    mutual_count = int(row.get("mutual_revert_count") or 0)
    top_reverter_share = float(row.get("top_reverter_share") or 0.0)
    strongest_pair_share = float(row.get("strongest_pair_share") or 0.0)
    revert_density = float(row.get("revert_density") or 0.0)

    penalty = 0.0
    title_is_routine = bool(ROUTINE_UPDATE_TITLE_RE.search(title))
    title_is_strong_routine = bool(STRONG_ROUTINE_UPDATE_TITLE_RE.search(title))
    title_is_season = bool(SEASON_TITLE_RE.search(title))
    if title_is_strong_routine:
        penalty += 0.32
    if title_is_routine:
        penalty += 0.20
    if title_is_season:
        penalty += 0.14

    if top_reverter_share >= 0.78 and revert_count >= 8:
        penalty += 0.24
    elif top_reverter_share >= 0.65 and revert_count >= 8:
        penalty += 0.12

    if strongest_pair_share >= 0.88 and mutual_pairs <= 1 and unique_editors <= 6:
        penalty += 0.16
    elif strongest_pair_share >= 0.75 and mutual_pairs <= 2 and unique_editors <= 10:
        penalty += 0.08

    if edit_count >= 80 and mutual_count <= 2:
        penalty += 0.16
    elif edit_count >= 45 and mutual_count <= 1:
        penalty += 0.10

    if title_is_strong_routine and mutual_pairs <= 4:
        penalty += 0.16
    elif title_is_routine and mutual_pairs <= 2:
        penalty += 0.12
    if title_is_routine and revert_density < 0.25 and peak_score < 160:
        penalty += 0.10

    return min(0.85, max(0.0, penalty))


def mutual_revert_metrics_from_edges(edges: Counter[tuple[str, str]]) -> tuple[int, int]:
    seen: set[frozenset[str]] = set()
    pairs = 0
    count = 0
    for (left, right), left_count in edges.items():
        pair_key = frozenset({left, right})
        if pair_key in seen:
            continue
        right_count = edges.get((right, left), 0)
        if right_count:
            seen.add(pair_key)
            pairs += 1
            count += min(left_count, right_count)
    return pairs, count


def _subtract_counter(target: Counter, values: Counter) -> None:
    target.subtract(values)
    for key in list(target):
        if target[key] <= 0:
            del target[key]


def _bucket_start(timestamp: datetime) -> datetime:
    return timestamp.replace(minute=0, second=0, microsecond=0)


def _is_bot(columns: list[str]) -> bool:
    return bool(
        columns[EVENT_USER_IS_BOT_BY_HISTORICAL]
        or columns[EVENT_USER_IS_BOT_BY]
        or _bool(columns[EVENT_USER_IS_BOT_COMPAT])
    )


def _bool(value: str) -> bool:
    return value.lower() == "true"


def _int(value: str) -> int | None:
    try:
        return int(value) if value != "" else None
    except ValueError:
        return None


def _timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def current_dump_url(snapshot: str, wiki: str, dump_name: str) -> str:
    return f"https://dumps.wikimedia.org/{wiki}/{snapshot}/{wiki}-{snapshot}-{dump_name}.sql.gz"


def current_dump_file_url(snapshot: str, wiki: str, filename: str) -> str:
    return f"https://dumps.wikimedia.org/{wiki}/{snapshot}/{filename}"


def list_current_dump_filenames(snapshot: str, wiki: str, pattern: re.Pattern[str]) -> list[str]:
    url = f"https://dumps.wikimedia.org/{wiki}/{snapshot}/"
    with httpx.Client(headers={"User-Agent": settings.user_agent}, timeout=30.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
    parser = LinkParser()
    parser.feed(response.text)
    # Wikimedia dump indexes emit absolute-path hrefs (e.g. "/enwiki/20260501/..."),
    # so normalize to basenames before matching against the supplied filename pattern.
    filenames = sorted({href.rsplit("/", 1)[-1] for href in parser.hrefs if pattern.match(href.rsplit("/", 1)[-1])})
    return filenames


DUMP_FORMAT_SUFFIXES = (".7z", ".bz2", ".gz")


def pick_preferred_dump_format(
    available_filenames: list[str],
    *,
    base_pattern: re.Pattern[str],
    prefer: tuple[str, ...] = (".7z", ".bz2", ".gz"),
) -> list[str]:
    """Group candidate dump filenames by their compression-stripped base name and,
    for each group, keep only the most preferred format that is present.

    Wikimedia ships the same logical dump in multiple compression formats
    (e.g. pages-meta-history.xml is offered as both .7z and .bz2). The 7z
    variant is typically an order of magnitude smaller, so prefer it to save
    bandwidth and disk. ``prefer`` lists formats from most to least preferred.
    """
    by_base: dict[str, list[str]] = defaultdict(list)
    for filename in available_filenames:
        stripped = filename
        for suffix in DUMP_FORMAT_SUFFIXES:
            if stripped.endswith(suffix):
                stripped = stripped[: -len(suffix)]
                break
        if not base_pattern.match(stripped):
            continue
        by_base[stripped].append(filename)
    selected: list[str] = []
    for base, candidates in sorted(by_base.items()):
        if len(candidates) == 1:
            selected.append(candidates[0])
            continue
        for preferred_suffix in prefer:
            match = next((name for name in candidates if name.endswith(preferred_suffix)), None)
            if match:
                selected.append(match)
                break
        else:
            selected.extend(candidates)
    return sorted(selected)


def open_compressed_dump(path: Path):
    """Open a Wikimedia dump for byte-stream reading, transparently handling
    .gz, .bz2, and .7z. The .7z path shells out to the ``7z`` binary because
    Python has no stdlib 7z support; a clear error is raised if it is missing.
    """
    suffix = "".join(path.suffixes[-1:])
    if suffix == ".gz":
        return gzip.open(path, "rb")
    if suffix == ".bz2":
        return bz2.open(path, "rb")
    if suffix == ".7z":
        binary = next((name for name in ("7z", "7za", "7zr", "p7zip") if shutil.which(name)), None)
        if not binary:
            raise RuntimeError(
                "Decompressing .7z dumps requires the '7z' binary (install p7zip-full / "
                "'brew install p7zip'). Falling back is not possible for 7z. File: " + str(path)
            )
        proc = subprocess.Popen(
            [binary, "x", "-so", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        return proc.stdout  # type: ignore[return-value]
    return path.open("rb")


def _human_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024.0
    return f"{value:.1f} TiB"


def _terminal_width(default: int = 80) -> int:
    try:
        import shutil as _shutil

        return _shutil.get_terminal_size((default, 24)).columns
    except Exception:
        return default


class DownloadProgress:
    """Carriage-return progress bar written to stderr. Works in non-TTY contexts
    by emitting one throttled status line per update interval instead of rewriting."""

    def __init__(self, label: str, *, total_bytes: int | None = None, update_interval: float = 0.5) -> None:
        self.label = label
        self.total_bytes = total_bytes
        self.update_interval = update_interval
        self.downloaded = 0
        self._last_update = 0.0
        self._started = 0.0
        self._is_tty = sys.stderr.isatty()

    def __enter__(self) -> "DownloadProgress":
        import time as _time

        self._started = _time.monotonic()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        import time as _time

        elapsed = _time.monotonic() - self._started
        if exc_type is None:
            self._render(final=True)
        sys.stderr.write("\n")
        sys.stderr.flush()
        if exc_type is None:
            log(
                f"download_progress_done label={self.label} "
                f"bytes={self.downloaded} elapsed_seconds={elapsed:.1f}"
            )

    def advance(self, chunk_bytes: int) -> None:
        import time as _time

        self.downloaded += chunk_bytes
        now = _time.monotonic()
        if now - self._last_update < self.update_interval and self.downloaded != self.total_bytes:
            return
        self._last_update = now
        self._render()

    def _render(self, *, final: bool = False) -> None:
        import time as _time

        elapsed = max(1e-6, _time.monotonic() - self._started)
        rate = self.downloaded / elapsed
        width = _terminal_width()
        label = self.label
        if len(label) > 32:
            label = label[:29] + "..."
        downloaded_h = _human_bytes(self.downloaded)
        if self.total_bytes and self.total_bytes > 0:
            pct = min(1.0, self.downloaded / self.total_bytes)
            total_h = _human_bytes(self.total_bytes)
            eta_seconds = (self.total_bytes - self.downloaded) / rate if rate > 0 else 0
            eta_h = _format_duration(eta_seconds)
            rate_h = _human_bytes(rate) + "/s"
            pct_str = f"{pct * 100:5.1f}%"
            right = f" {pct_str}  {downloaded_h} / {total_h}  {rate_h}  ETA {eta_h}"
            bar_budget = max(0, width - len(label) - len(right) - 4)
            filled = int(bar_budget * pct)
            bar = "[" + "#" * filled + ">" + "-" * max(0, bar_budget - filled - 1) + "]"
            line = f"{label} {bar}{right}"
        else:
            rate_h = _human_bytes(rate) + "/s"
            right = f" {downloaded_h}  {rate_h}"
            line = f"{label}{right}"
        line = line[:width]
        if self._is_tty:
            sys.stderr.write("\r" + line + " ")
        else:
            sys.stderr.write(line + "\n")
        sys.stderr.flush()


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"



def _remote_content_length(url: str) -> int | None:
    try:
        with httpx.Client(
            headers={"User-Agent": settings.user_agent}, timeout=30.0, follow_redirects=True
        ) as client:
            head = client.head(url)
            head.raise_for_status()
            value = head.headers.get("Content-Length")
            return int(value) if value and value.isdigit() else None
    except Exception:
        return None


def download_current_dump_file(
    *,
    snapshot: str,
    wiki: str,
    filename: str,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / filename
    if target.exists() and target.stat().st_size > 0:
        return target
    partial = target.with_suffix(target.suffix + ".part")
    url = current_dump_file_url(snapshot, wiki, filename)
    log(f"download_start url={url} target={target}")
    total = _remote_content_length(url)
    with httpx.stream(
        "GET",
        url,
        headers={"User-Agent": settings.user_agent},
        timeout=60.0,
        follow_redirects=True,
    ) as response:
        response.raise_for_status()
        if total is None:
            header_total = response.headers.get("Content-Length")
            total = int(header_total) if header_total and header_total.isdigit() else None
        with partial.open("wb") as file, DownloadProgress(filename, total_bytes=total) as progress:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                file.write(chunk)
                progress.advance(len(chunk))
    partial.replace(target)
    log(f"download_done target={target} bytes={target.stat().st_size}")
    return target


def download_current_sql_dump(
    *,
    snapshot: str,
    wiki: str,
    dump_name: str,
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{wiki}-{snapshot}-{dump_name}.sql.gz"
    if target.exists() and target.stat().st_size > 0:
        return target
    partial = target.with_suffix(target.suffix + ".part")
    url = current_dump_url(snapshot, wiki, dump_name)
    log(f"download_start url={url} target={target}")
    total = _remote_content_length(url)
    with httpx.stream(
        "GET",
        url,
        headers={"User-Agent": settings.user_agent},
        timeout=60.0,
        follow_redirects=True,
    ) as response:
        response.raise_for_status()
        if total is None:
            header_total = response.headers.get("Content-Length")
            total = int(header_total) if header_total and header_total.isdigit() else None
        with partial.open("wb") as file, DownloadProgress(target.name, total_bytes=total) as progress:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                file.write(chunk)
                progress.advance(len(chunk))
    partial.replace(target)
    log(f"download_done target={target} bytes={target.stat().st_size}")
    return target


def import_page_restrictions_dump(
    *,
    path: Path,
    wiki: str,
    source: str,
) -> int:
    signals = parse_page_restrictions_dump(path, source=source)
    init_db()
    with SessionLocal() as session:
        replace_page_admin_signals(session, wiki, signals)
    return len(signals)


def import_protection_log_dumps(
    *,
    paths: list[Path],
    wiki: str,
    source: str,
    open_until: datetime | None = None,
    status_file: Path | None = None,
) -> int:
    signals = parse_protection_log_dumps(
        paths,
        source=source,
        open_until=open_until or datetime.now(timezone.utc),
        status_file=status_file,
    )
    init_db()
    with SessionLocal() as session:
        replace_page_admin_title_signals(session, wiki, signals)
    return len(signals)


def parse_page_restrictions_dump(path: Path, *, source: str = "") -> list[dict[str, object]]:
    signal_map: dict[int, dict[str, object]] = {}
    for row in iter_mysql_insert_rows(path, "page_restrictions"):
        if len(row) < 6:
            continue
        page_id = _int(str(row[0] or ""))
        if not page_id:
            continue
        restriction_type = str(row[1] or "")
        restriction_level = str(row[2] or "")
        cascade = str(row[3] or "") == "1"
        expiry = str(row[4] or "")
        signal = signal_map.setdefault(
            page_id,
            {
                "page_id": page_id,
                "restriction_count": 0,
                "restriction_types_set": set(),
                "restriction_levels_set": set(),
                "has_extendedconfirmed": False,
                "has_sysop": False,
                "has_cascade": False,
                "restriction_expiry": "",
                "protection_event_count": 0,
                "protection_days": 0.0,
                "source": source,
            },
        )
        signal["restriction_count"] = int(signal["restriction_count"]) + 1
        signal["restriction_types_set"].add(restriction_type)  # type: ignore[union-attr]
        signal["restriction_levels_set"].add(restriction_level)  # type: ignore[union-attr]
        normalized_level = restriction_level.lower().replace("-", "")
        signal["has_extendedconfirmed"] = bool(signal["has_extendedconfirmed"]) or normalized_level == "extendedconfirmed"
        signal["has_sysop"] = bool(signal["has_sysop"]) or normalized_level == "sysop"
        signal["has_cascade"] = bool(signal["has_cascade"]) or cascade
        signal["restriction_expiry"] = max_restriction_expiry(str(signal["restriction_expiry"]), expiry)

    signals: list[dict[str, object]] = []
    for signal in signal_map.values():
        types = sorted(str(value) for value in signal.pop("restriction_types_set"))  # type: ignore[arg-type]
        levels = sorted(str(value) for value in signal.pop("restriction_levels_set"))  # type: ignore[arg-type]
        signal["restriction_types"] = ", ".join(types)
        signal["restriction_levels"] = ", ".join(levels)
        signals.append(signal)
    return sorted(signals, key=lambda item: int(item["page_id"]))


def parse_protection_log_dumps(
    paths: list[Path],
    *,
    source: str = "",
    open_until: datetime,
    status_file: Path | None = None,
) -> list[dict[str, object]]:
    events_by_title: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    events_seen = 0
    for index, path in enumerate(paths, start=1):
        if status_file:
            status_file.parent.mkdir(parents=True, exist_ok=True)
            status_file.write_text(
                f"{datetime.now(timezone.utc).isoformat()} parsing {index}/{len(paths)} {path} "
                f"events={events_seen} titles={len(events_by_title)}\n",
                encoding="utf-8",
            )
        for event in iter_protection_log_events(path):
            title = normalize_history_title(event["page_title"])
            if not title or ":" in title:
                continue
            events_by_title[title].append((event["timestamp"], event["action"]))  # type: ignore[arg-type]
            events_seen += 1
    signals = [
        protection_events_to_signal(title, events, source=source, open_until=open_until)
        for title, events in events_by_title.items()
    ]
    signals = [signal for signal in signals if int(signal["protection_event_count"]) > 0]
    if status_file:
        status_file.write_text(
            f"{datetime.now(timezone.utc).isoformat()} complete files={len(paths)} "
            f"events={events_seen} titles={len(signals)}\n",
            encoding="utf-8",
        )
    return sorted(signals, key=lambda item: str(item["page_title"]))


def iter_protection_log_events(path: Path) -> Iterable[dict[str, object]]:
    with open_compressed_dump(path) as file:
        for _, element in ET.iterparse(file, events=("end",)):
            if xml_local_name(element.tag) != "logitem":
                continue
            log_type = xml_child_text(element, "type")
            if log_type != "protect":
                element.clear()
                continue
            timestamp = parse_log_timestamp(xml_child_text(element, "timestamp"))
            title = xml_child_text(element, "logtitle")
            action = xml_child_text(element, "action")
            if timestamp and title and action:
                yield {
                    "timestamp": timestamp,
                    "page_title": title,
                    "action": action,
                }
            element.clear()


def protection_events_to_signal(
    title: str,
    events: list[tuple[datetime, str]],
    *,
    source: str,
    open_until: datetime,
) -> dict[str, object]:
    sorted_events = sorted(events, key=lambda item: item[0])
    active_start: datetime | None = None
    protected_seconds = 0.0
    first_protection_at: datetime | None = None
    last_protection_at: datetime | None = None
    protection_event_count = 0
    for timestamp, action in sorted_events:
        if action in {"protect", "modify", "move_prot"}:
            protection_event_count += 1
            first_protection_at = first_protection_at or timestamp
            last_protection_at = timestamp
            if active_start is None:
                active_start = timestamp
        elif action == "unprotect":
            protection_event_count += 1
            last_protection_at = timestamp
            if active_start is not None and timestamp > active_start:
                protected_seconds += (timestamp - active_start).total_seconds()
                active_start = None
    if active_start is not None and open_until > active_start:
        protected_seconds += (open_until - active_start).total_seconds()
    return {
        "page_title": title,
        "protection_event_count": protection_event_count,
        "protection_days": round(protected_seconds / 86_400, 2),
        "first_protection_at": first_protection_at,
        "last_protection_at": last_protection_at,
        "source": source,
    }


def xml_child_text(element: ET.Element, child_name: str) -> str:
    for child in element:
        if xml_local_name(child.tag) == child_name:
            return child.text or ""
    return ""


def xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_log_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def max_restriction_expiry(left: str, right: str) -> str:
    if right.lower() == "infinity" or left.lower() == "infinity":
        return "infinity"
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


def iter_mysql_insert_rows(path: Path, table_name: str) -> Iterable[list[str | None]]:
    opener = gzip.open if path.suffix == ".gz" else open
    prefix = f"INSERT INTO `{table_name}` VALUES "
    with opener(path, "rt", encoding="utf-8", errors="replace", newline="") as file:
        for line in file:
            if not line.startswith(prefix):
                continue
            values = line[len(prefix):].strip()
            if values.endswith(";"):
                values = values[:-1]
            for tuple_text in iter_mysql_value_tuples(values):
                yield split_mysql_tuple(tuple_text)


def iter_mysql_value_tuples(values: str) -> Iterable[str]:
    in_quote = False
    escape = False
    depth = 0
    start: int | None = None
    for index, char in enumerate(values):
        if in_quote:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_quote = False
            continue
        if char == "'":
            in_quote = True
        elif char == "(":
            if depth == 0:
                start = index + 1
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and start is not None:
                yield values[start:index]
                start = None


def split_mysql_tuple(tuple_text: str) -> list[str | None]:
    values: list[str | None] = []
    buffer: list[str] = []
    in_quote = False
    escape = False
    for char in tuple_text:
        if in_quote:
            if escape:
                buffer.append(mysql_unescape_char(char))
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_quote = False
            else:
                buffer.append(char)
            continue
        if char == "'":
            in_quote = True
        elif char == ",":
            values.append(mysql_field_value("".join(buffer)))
            buffer = []
        else:
            buffer.append(char)
    values.append(mysql_field_value("".join(buffer)))
    return values


def mysql_field_value(value: str) -> str | None:
    value = value.strip()
    if value.upper() == "NULL":
        return None
    return value


def mysql_unescape_char(char: str) -> str:
    return {
        "0": "\0",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "b": "\b",
        "Z": "\x1a",
        "\\": "\\",
        "'": "'",
        '"': '"',
    }.get(char, char)


def local_history_dump_files(
    *,
    dump_dir: Path,
    snapshot: str,
    wiki: str,
    start_partition: str | None = None,
    stop_partition: str | None = None,
) -> list[Path]:
    prefix = f"{snapshot}.{wiki}."
    suffix = ".tsv.bz2"
    files = sorted(dump_dir.glob(f"{prefix}*{suffix}"))
    result = []
    for path in files:
        partition = partition_from_history_dump_path(path, snapshot=snapshot, wiki=wiki)
        if start_partition and partition < start_partition:
            continue
        if stop_partition and partition > stop_partition:
            continue
        result.append(path)
    return result


def partition_from_history_dump_path(path: Path, *, snapshot: str, wiki: str) -> str:
    name = path.name
    prefix = f"{snapshot}.{wiki}."
    suffix = ".tsv.bz2"
    if not name.startswith(prefix) or not name.endswith(suffix):
        raise ValueError(f"Unexpected mediawiki_history dump filename: {path}")
    return name[len(prefix) : -len(suffix)]


def log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat()} {message}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Work with downloadable mediawiki_history dumps.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List dump partitions for a snapshot/wiki.")
    list_parser.add_argument("--snapshot", required=True)
    list_parser.add_argument("--wiki", default="enwiki")

    download_parser = subparsers.add_parser("download", help="Download one dump partition.")
    download_parser.add_argument("--snapshot", required=True)
    download_parser.add_argument("--wiki", default="enwiki")
    download_parser.add_argument("--partition", required=True)
    download_parser.add_argument("--output-dir", type=Path, default=Path("data/dumps"))

    prefetch_parser = subparsers.add_parser("prefetch", help="Bulk-prefetch dump partitions with rsync.")
    prefetch_parser.add_argument("--snapshot", required=True)
    prefetch_parser.add_argument("--wiki", default="enwiki")
    prefetch_parser.add_argument("--output-dir", type=Path, default=Path("data/dumps"))
    prefetch_parser.add_argument("--mirror", default=DEFAULT_RSYNC_MIRROR)
    prefetch_parser.add_argument("--start-partition")
    prefetch_parser.add_argument("--stop-partition")
    prefetch_parser.add_argument("--dry-run", action="store_true")

    process_parser = subparsers.add_parser("process", help="Process local .tsv.bz2 files.")
    process_parser.add_argument("files", type=Path, nargs="+")
    process_parser.add_argument("--period", required=True)
    process_parser.add_argument("--limit", type=int, default=100)
    process_parser.add_argument("--min-score", type=float, default=40.0)
    process_parser.add_argument("--namespace", type=int, default=0)
    process_parser.add_argument("--dry-run", action="store_true")

    rebuild_local_parser = subparsers.add_parser(
        "rebuild-local",
        help="Reprocess already downloaded mediawiki_history partitions from local disk.",
    )
    rebuild_local_parser.add_argument("--snapshot", required=True)
    rebuild_local_parser.add_argument("--wiki", default="enwiki")
    rebuild_local_parser.add_argument("--dump-dir", type=Path, default=Path("data/dumps"))
    rebuild_local_parser.add_argument("--period-prefix", default="history")
    rebuild_local_parser.add_argument("--limit", type=int, default=100)
    rebuild_local_parser.add_argument("--min-score", type=float, default=0.0)
    rebuild_local_parser.add_argument("--start-partition")
    rebuild_local_parser.add_argument("--stop-partition")
    rebuild_local_parser.add_argument("--status-file", type=Path, default=Path("data/logs/method-rebuild-status.txt"))

    admin_parser = subparsers.add_parser(
        "import-admin-signals",
        help="Import page-level governance/protection signals from local Wikimedia SQL dumps.",
    )
    admin_parser.add_argument("--snapshot", required=True, help="Regular dump snapshot, for example 20260501.")
    admin_parser.add_argument("--wiki", default="enwiki")
    admin_parser.add_argument("--dump-dir", type=Path, default=Path("data/admin-dumps"))
    admin_parser.add_argument("--restrictions", type=Path)
    admin_parser.add_argument("--download", action="store_true", default=True)
    admin_parser.add_argument("--no-download", dest="download", action="store_false")

    protection_parser = subparsers.add_parser(
        "import-protection-log",
        help="Import article protection durations from pages-logging XML dumps.",
    )
    protection_parser.add_argument("files", type=Path, nargs="*")
    protection_parser.add_argument("--snapshot", required=True, help="Regular dump snapshot, for example 20260501.")
    protection_parser.add_argument("--wiki", default="enwiki")
    protection_parser.add_argument("--dump-dir", type=Path, default=Path("data/admin-dumps"))
    protection_parser.add_argument("--download", action="store_true", default=False)
    protection_parser.add_argument(
        "--prefer-7z",
        dest="prefer_7z",
        action="store_true",
        default=True,
        help="When multiple compression formats are available, prefer the smallest (7z). Default on.",
    )
    protection_parser.add_argument(
        "--no-7z",
        dest="prefer_7z",
        action="store_false",
        help="Disable 7z preference and fall back to gz/bz2 ordering.",
    )
    protection_parser.add_argument("--source")
    protection_parser.add_argument("--status-file", type=Path, default=Path("data/logs/protection-log-status.txt"))

    recompute_parser = subparsers.add_parser(
        "recompute",
        help="Regenerate historical scoreboards from locally stored parsed aggregates.",
    )
    recompute_parser.add_argument("--period")
    recompute_parser.add_argument("--limit", type=int, default=100)
    recompute_parser.add_argument("--min-score", type=float, default=40.0)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Download and process every partition for a snapshot/wiki, one partition at a time.",
    )
    backfill_parser.add_argument("--snapshot", required=True)
    backfill_parser.add_argument("--wiki", default="enwiki")
    backfill_parser.add_argument("--output-dir", type=Path, default=Path("data/dumps"))
    backfill_parser.add_argument("--period-prefix", default="history")
    backfill_parser.add_argument("--limit", type=int, default=100)
    backfill_parser.add_argument("--min-score", type=float, default=40.0)
    backfill_parser.add_argument("--namespace", type=int, default=0)
    backfill_parser.add_argument("--keep-downloads", action="store_true", default=True)
    backfill_parser.add_argument("--discard-downloads", dest="keep_downloads", action="store_false")
    backfill_parser.add_argument("--sleep-seconds", type=float, default=2.0)
    backfill_parser.add_argument("--start-partition")
    backfill_parser.add_argument("--stop-partition")
    backfill_parser.add_argument("--workers", type=int, default=1)

    args = parser.parse_args()
    if args.command == "list":
        for partition in list_dump_partitions(args.snapshot, args.wiki):
            print(partition)
    elif args.command == "download":
        print(download_dump(args.snapshot, args.wiki, args.partition, args.output_dir))
    elif args.command == "prefetch":
        prefetch_history_dumps(
            snapshot=args.snapshot,
            wiki=args.wiki,
            output_dir=args.output_dir,
            mirror=args.mirror,
            start_partition=args.start_partition,
            stop_partition=args.stop_partition,
            dry_run=args.dry_run,
        )
    elif args.command == "process":
        result = process_history_files(
            args.files,
            period=args.period,
            limit=args.limit,
            min_score=args.min_score,
            namespace=args.namespace,
            write=not args.dry_run,
        )
        print(
            f"period={result.period} rows_seen={result.rows_seen} "
            f"revisions_seen={result.revisions_seen} pages_scored={result.pages_scored} "
            f"rows_written={result.rows_written}"
        )
    elif args.command == "rebuild-local":
        files = local_history_dump_files(
            dump_dir=args.dump_dir,
            snapshot=args.snapshot,
            wiki=args.wiki,
            start_partition=args.start_partition,
            stop_partition=args.stop_partition,
        )
        args.status_file.parent.mkdir(parents=True, exist_ok=True)
        for index, path in enumerate(files, start=1):
            partition = partition_from_history_dump_path(path, snapshot=args.snapshot, wiki=args.wiki)
            period = f"{args.period_prefix}:{args.snapshot}:{partition}"
            args.status_file.write_text(
                f"{datetime.now(timezone.utc).isoformat()} {index}/{len(files)} {period} {path}\n",
                encoding="utf-8",
            )
            log(f"rebuild_local_start index={index}/{len(files)} period={period} path={path}")
            result = process_history_files(
                [path],
                period=period,
                limit=args.limit,
                min_score=args.min_score,
                namespace=0,
                write=True,
            )
            log(
                f"rebuild_local_done index={index}/{len(files)} period={period} "
                f"rows_seen={result.rows_seen} revisions_seen={result.revisions_seen} "
                f"pages_scored={result.pages_scored} rows_written={result.rows_written}"
            )
        args.status_file.write_text(
            f"{datetime.now(timezone.utc).isoformat()} complete {len(files)}/{len(files)}\n",
            encoding="utf-8",
        )
    elif args.command == "import-admin-signals":
        restrictions_path = args.restrictions
        if restrictions_path is None:
            restrictions_path = args.dump_dir / f"{args.wiki}-{args.snapshot}-page_restrictions.sql.gz"
        if args.download:
            restrictions_path = download_current_sql_dump(
                snapshot=args.snapshot,
                wiki=args.wiki,
                dump_name="page_restrictions",
                output_dir=args.dump_dir,
            )
        count = import_page_restrictions_dump(
            path=restrictions_path,
            wiki=args.wiki,
            source=f"{args.wiki}-{args.snapshot}-page_restrictions",
        )
        print(f"imported_page_admin_signals={count}")
    elif args.command == "import-protection-log":
        files = list(args.files)
        if args.download:
            # Match split pages-logging shards in any supported compression format
            # (gz/bz2/7z). Avoid the unsplit logging.xml.gz so we do not double-ingest.
            candidate_pattern = re.compile(
                rf"^{re.escape(args.wiki)}-{re.escape(args.snapshot)}-pages-logging\d+\.xml"
            )
            all_filenames = list_current_dump_filenames(args.snapshot, args.wiki, candidate_pattern)
            prefer = (".7z",) if args.prefer_7z else (".7z", ".bz2", ".gz")
            filenames = pick_preferred_dump_format(
                all_filenames,
                base_pattern=re.compile(
                    rf"^{re.escape(args.wiki)}-{re.escape(args.snapshot)}-pages-logging\d+\.xml$"
                ),
                prefer=prefer,
            )
            if not filenames:
                raise SystemExit(
                    f"No pages-logging shards found for snapshot={args.snapshot} wiki={args.wiki}."
                )
            files = [
                download_current_dump_file(
                    snapshot=args.snapshot,
                    wiki=args.wiki,
                    filename=filename,
                    output_dir=args.dump_dir,
                )
                for filename in filenames
            ]
        if not files:
            raise SystemExit("No pages-logging files provided or downloaded.")
        count = import_protection_log_dumps(
            paths=files,
            wiki=args.wiki,
            source=args.source or f"{args.wiki}-{args.snapshot}-pages-logging",
            open_until=datetime.strptime(args.snapshot, "%Y%m%d").replace(tzinfo=timezone.utc),
            status_file=args.status_file,
        )
        print(f"imported_page_admin_title_signals={count}")
    elif args.command == "recompute":
        periods = recompute_scoreboard_from_aggregates(
            period=args.period,
            limit=args.limit,
            min_score=args.min_score,
        )
        print(f"recomputed_periods={len(periods)}")
        for period in periods:
            print(period)
    elif args.command == "backfill":
        try:
            backfill_snapshot(
                snapshot=args.snapshot,
                wiki=args.wiki,
                output_dir=args.output_dir,
                period_prefix=args.period_prefix,
                limit=args.limit,
                min_score=args.min_score,
                namespace=args.namespace,
                keep_downloads=args.keep_downloads,
                sleep_seconds=args.sleep_seconds,
                start_partition=args.start_partition,
                stop_partition=args.stop_partition,
                workers=max(1, args.workers),
            )
        except KeyboardInterrupt:
            log("backfill_interrupted")
            sys.exit(130)


if __name__ == "__main__":
    main()
