from __future__ import annotations

import argparse
import bz2
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from datetime import timedelta
from html.parser import HTMLParser
import math
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Iterable

import httpx

from .config import settings
from .repository import (
    historical_aggregate_period_exists,
    historical_aggregate_periods,
    historical_processed_period_exists,
    load_historical_aggregates,
    mark_historical_period_processed,
    replace_historical_aggregates,
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
REVISION_IS_IDENTITY_REVERTED = 72
REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID = 73
REVISION_IS_IDENTITY_REVERT = 75
HISTORICAL_BUCKET_MINUTES = 60
HISTORICAL_WINDOW_HOURS = 24
HISTORICAL_REVERT_EDGE_THRESHOLD = 4
HISTORICAL_ADDITIONAL_PAIR_WEIGHT = 0.25


@dataclass
class HistoricalRevision:
    wiki: str
    page_id: int
    page_title: str
    timestamp: datetime
    user_text: str
    rev_id: int
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
        self._bucket(revision.timestamp).add_revision(revision)

    def add_revert(self, reverter: str, reverted: str, timestamp: datetime | None = None) -> None:
        if reverter and reverted and reverter != reverted:
            self.revert_edges[(reverter, reverted)] += 1
            if timestamp is not None:
                self._bucket(timestamp).add_revert(reverter, reverted)

    def _bucket(self, timestamp: datetime) -> HistoricalBucket:
        bucket_start = _bucket_start(timestamp)
        return self.buckets.setdefault(bucket_start, HistoricalBucket())


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
        rev_id=rev_id,
        is_identity_reverted=_bool(columns[REVISION_IS_IDENTITY_REVERTED]),
        first_identity_reverting_revision_id=_int(
            columns[REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID]
        ),
        is_identity_revert=_bool(columns[REVISION_IS_IDENTITY_REVERT]),
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
    pending_reverts: dict[int, list[HistoricalRevision]] = defaultdict(list)
    rows_seen = 0
    revisions_seen = 0

    for path in paths:
        with bz2.open(path, "rt", encoding="utf-8", newline="") as file:
            for line in file:
                rows_seen += 1
                revision = parse_history_row(line.rstrip("\n").split("\t"), namespace)
                if revision is None:
                    continue
                revisions_seen += 1
                key = (revision.wiki, revision.page_id)
                aggregate = aggregates.setdefault(
                    key,
                    PageHistoricalAggregate(
                        wiki=revision.wiki,
                        page_id=revision.page_id,
                        page_title=revision.page_title,
                    ),
                )
                aggregate.add_revision(revision)

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
        if row["peak_score"] >= min_score
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
                if row["peak_score"] >= min_score
            ]
            selected = rank_historical_rows(scored_rows, limit)
            replace_scoreboard_snapshot(session, selected_period, selected)
            rewritten.append(selected_period)
    return rewritten


def rank_historical_rows(rows: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    rows.sort(
        key=lambda row: (
            row["peak_score"],
            row["mutual_revert_pairs"],
            row["revert_count"],
        ),
        reverse=True,
    )
    return rows[:limit]


def aggregate_to_record(aggregate: PageHistoricalAggregate) -> dict[str, object] | None:
    revert_count = sum(mutual_revert_edges(effective_revert_edges(aggregate.revert_edges)).values())
    if revert_count == 0:
        return None
    mutual_pairs, _ = mutual_revert_metrics_from_edges(effective_revert_edges(aggregate.revert_edges))
    return {
        "wiki": aggregate.wiki,
        "page_id": aggregate.page_id,
        "page_title": aggregate.page_title,
        "edit_count": aggregate.edit_count,
        "unique_editors": aggregate.unique_editor_count if aggregate.unique_editor_count is not None else len(aggregate.editors),
        "revert_count": revert_count,
        "mutual_revert_pairs": mutual_pairs,
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


def record_to_aggregate(record: dict[str, object]) -> PageHistoricalAggregate:
    aggregate = PageHistoricalAggregate(
        wiki=str(record["wiki"]),
        page_id=int(record["page_id"]),
        page_title=str(record["page_title"]),
        edit_count=int(record["edit_count"]),
        unique_editor_count=int(record["unique_editors"]),
        first_timestamp=record.get("first_timestamp"),  # type: ignore[arg-type]
        last_timestamp=record.get("last_timestamp"),  # type: ignore[arg-type]
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
    mutual_pairs, _ = mutual_revert_metrics_from_edges(effective_edges)
    return {
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
        "revert_density": round(revert_density, 4),
    }


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
