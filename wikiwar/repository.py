from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any

from sqlalchemy import delete, desc, func, insert, select, tuple_, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .domain import EditEvent, RevertSignal, WindowScore
from .schema import (
    dumps,
    edits,
    historical_evidence_cache,
    historical_hourly_buckets,
    historical_page_aggregates,
    historical_processed_periods,
    loads,
    page_windows,
    raw_events,
    reverts,
    scoreboard_snapshots,
    war_episodes,
)


HISTORICAL_MONTH_RE = re.compile(r"^history:(?P<snapshot>\d{4}-\d{2}):(?P<month>\d{4}-\d{2})$")
HISTORICAL_YEAR_RE = re.compile(r"^history-year:(?P<snapshot>\d{4}-\d{2}):(?P<year>\d{4})$")


def save_raw_event(session: Session, stream_id: str | None, received_at: datetime, payload: dict[str, Any]) -> bool:
    try:
        session.execute(
            insert(raw_events).values(
                stream_id=stream_id,
                received_at=received_at,
                payload_json=dumps(payload),
            )
        )
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False


def save_edit(session: Session, edit: EditEvent) -> bool:
    try:
        session.execute(
            insert(edits).values(
                wiki=edit.wiki,
                page_id=edit.page_id,
                page_title=edit.page_title,
                namespace=edit.namespace,
                rev_id=edit.rev_id,
                parent_rev_id=edit.parent_rev_id,
                timestamp=edit.timestamp,
                user_id=edit.user_id,
                user_text=edit.user_text,
                user_is_bot=edit.user_is_bot,
                user_is_anonymous=edit.user_is_anonymous,
                comment=edit.comment,
                tags=dumps(edit.tags),
                minor=edit.minor,
                old_len=edit.old_len,
                new_len=edit.new_len,
                source=edit.source,
            )
        )
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False


def save_revert(session: Session, revert: RevertSignal) -> bool:
    try:
        session.execute(
            insert(reverts).values(
                wiki=revert.wiki,
                page_id=revert.page_id,
                rev_id=revert.rev_id,
                reverter_user=revert.reverter_user,
                reverted_user=revert.reverted_user,
                reverted_rev_id=revert.reverted_rev_id,
                detector=revert.detector,
                confidence=revert.confidence,
                timestamp=revert.timestamp,
            )
        )
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False


def fetch_page_edits(session: Session, wiki: str, page_id: int, since: datetime) -> list[dict[str, Any]]:
    rows = session.execute(
        select(edits).where(
            edits.c.wiki == wiki,
            edits.c.page_id == page_id,
            edits.c.timestamp >= since,
        )
    ).mappings()
    result = []
    for row in rows:
        item = dict(row)
        item["tags"] = loads(item.get("tags")) or []
        result.append(item)
    return result


def fetch_page_reverts(session: Session, wiki: str, page_id: int, since: datetime) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in session.execute(
            select(reverts).where(
                reverts.c.wiki == wiki,
                reverts.c.page_id == page_id,
                reverts.c.timestamp >= since,
            )
        ).mappings()
    ]


def save_window_score(session: Session, score: WindowScore) -> None:
    session.execute(
        insert(page_windows).values(
            wiki=score.wiki,
            page_id=score.page_id,
            page_title=score.page_title,
            window_start=score.window_start,
            window_size=score.window_size,
            edit_count=score.edit_count,
            human_edit_count=score.human_edit_count,
            unique_human_editors=score.unique_human_editors,
            revert_count=score.revert_count,
            mutual_revert_count=score.mutual_revert_count,
            mutual_revert_pairs=score.mutual_revert_pairs,
            top_reverter_share=score.top_reverter_share,
            revert_density=score.revert_density,
            edit_velocity_z=score.edit_velocity_z,
            conflict_score=score.conflict_score,
            severity=score.severity,
            feature_json=dumps(score.features),
        )
    )
    session.commit()


def update_war_episode(session: Session, score: WindowScore) -> None:
    now = datetime.now(timezone.utc)
    active = session.execute(
        select(war_episodes)
        .where(
            war_episodes.c.wiki == score.wiki,
            war_episodes.c.page_id == score.page_id,
            war_episodes.c.status == "active",
        )
        .order_by(desc(war_episodes.c.started_at))
        .limit(1)
    ).mappings().first()

    if score.conflict_score >= 60:
        if active:
            session.execute(
                update(war_episodes)
                .where(war_episodes.c.id == active["id"])
                .values(
                    page_title=score.page_title,
                    peak_score=max(float(active["peak_score"]), score.conflict_score),
                    score_area=float(active["score_area"]) + max(0.0, score.conflict_score - 40.0),
                    total_edits=score.human_edit_count,
                    total_reverts=score.revert_count,
                    total_mutual_reverts=score.mutual_revert_count,
                    participants=score.unique_human_editors,
                )
            )
        else:
            session.execute(
                insert(war_episodes).values(
                    wiki=score.wiki,
                    page_id=score.page_id,
                    page_title=score.page_title,
                    started_at=now,
                    ended_at=None,
                    peak_score=score.conflict_score,
                    score_area=max(0.0, score.conflict_score - 40.0),
                    total_edits=score.human_edit_count,
                    total_reverts=score.revert_count,
                    total_mutual_reverts=score.mutual_revert_count,
                    participants=score.unique_human_editors,
                    status="active",
                )
            )
    elif active and score.conflict_score < 40:
        session.execute(
            update(war_episodes)
            .where(war_episodes.c.id == active["id"])
            .values(status="resolved", ended_at=now)
        )
    session.commit()


def latest_window_candidates(session: Session, window_size: str = "24h", limit: int = 50) -> list[dict[str, Any]]:
    latest_ids = (
        select(func.max(page_windows.c.id).label("id"))
        .where(page_windows.c.window_size == window_size)
        .group_by(page_windows.c.wiki, page_windows.c.page_id)
        .subquery()
    )
    rows = session.execute(
        select(page_windows)
        .join(latest_ids, page_windows.c.id == latest_ids.c.id)
        .order_by(desc(page_windows.c.conflict_score), desc(page_windows.c.revert_count))
        .limit(limit)
    ).mappings()
    return [_decode_window(dict(row)) for row in rows]


def recent_edits_for_page(session: Session, wiki: str, page_id: int, limit: int = 50) -> list[dict[str, Any]]:
    rows = session.execute(
        select(edits)
        .where(edits.c.wiki == wiki, edits.c.page_id == page_id)
        .order_by(desc(edits.c.timestamp))
        .limit(limit)
    ).mappings()
    result = []
    for row in rows:
        item = dict(row)
        item["tags"] = loads(item.get("tags")) or []
        result.append(item)
    return result


def recent_windows_for_page(session: Session, wiki: str, page_id: int, limit: int = 60) -> list[dict[str, Any]]:
    rows = session.execute(
        select(page_windows)
        .where(
            page_windows.c.wiki == wiki,
            page_windows.c.page_id == page_id,
            page_windows.c.window_size == "24h",
        )
        .order_by(desc(page_windows.c.id))
        .limit(limit)
    ).mappings()
    return [_decode_window(dict(row)) for row in rows][::-1]


def active_episodes(session: Session, limit: int = 50) -> list[dict[str, Any]]:
    rows = session.execute(
        select(war_episodes)
        .where(war_episodes.c.status == "active")
        .order_by(desc(war_episodes.c.peak_score), desc(war_episodes.c.started_at))
        .limit(limit)
    ).mappings()
    return [dict(row) for row in rows]


def episode_scoreboard(session: Session, period_hours: int = 24, limit: int = 50) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(hours=period_hours)
    rows = session.execute(
        select(war_episodes)
        .where(war_episodes.c.started_at >= since)
        .order_by(
            desc(war_episodes.c.total_reverts),
            desc(war_episodes.c.total_mutual_reverts),
            desc(war_episodes.c.peak_score),
            desc(war_episodes.c.score_area),
        )
        .limit(limit)
    ).mappings()
    return [dict(row) for row in rows]


def replace_scoreboard_snapshot(session: Session, period: str, rows: list[dict[str, Any]]) -> None:
    session.execute(delete(scoreboard_snapshots).where(scoreboard_snapshots.c.period == period))
    for rank, row in enumerate(rows, start=1):
        session.execute(
            insert(scoreboard_snapshots).values(
                period=period,
                rank=rank,
                wiki=row["wiki"],
                page_id=row["page_id"],
                page_title=row["page_title"],
                score_area=row["score_area"],
                peak_score=row["peak_score"],
                war_minutes=row["war_minutes"],
                episode_count=row["episode_count"],
            )
        )
    session.commit()


def replace_historical_aggregates(session: Session, period: str, aggregates: list[dict[str, Any]]) -> None:
    session.execute(delete(historical_hourly_buckets).where(historical_hourly_buckets.c.period == period))
    session.execute(delete(historical_page_aggregates).where(historical_page_aggregates.c.period == period))
    page_values = []
    bucket_values = []
    for aggregate in aggregates:
        page_values.append(
            {
                "period": period,
                "wiki": aggregate["wiki"],
                "page_id": aggregate["page_id"],
                "page_title": aggregate["page_title"],
                "edit_count": aggregate["edit_count"],
                "unique_editors": aggregate["unique_editors"],
                "revert_count": aggregate["revert_count"],
                "mutual_revert_pairs": aggregate["mutual_revert_pairs"],
                "raw_reverted_count": aggregate.get("raw_reverted_count", 0),
                "raw_revert_count": aggregate.get("raw_revert_count", 0),
                "talk_page_id": aggregate.get("talk_page_id"),
                "talk_edit_count": aggregate.get("talk_edit_count", 0),
                "talk_unique_editors": aggregate.get("talk_unique_editors", 0),
                "talk_text_bytes": aggregate.get("talk_text_bytes", 0),
                "first_timestamp": aggregate.get("first_timestamp"),
                "last_timestamp": aggregate.get("last_timestamp"),
            }
        )
        for bucket in aggregate["buckets"]:
            bucket_values.append(
                {
                    "period": period,
                    "wiki": aggregate["wiki"],
                    "page_id": aggregate["page_id"],
                    "bucket_start": bucket["bucket_start"],
                    "edit_count": bucket["edit_count"],
                    "editors_json": dumps(bucket["editors"]),
                    "revert_edges_json": dumps(bucket["revert_edges"]),
                }
            )
    if page_values:
        session.execute(insert(historical_page_aggregates), page_values)
    if bucket_values:
        session.execute(insert(historical_hourly_buckets), bucket_values)
    session.commit()


def mark_historical_period_processed(
    session: Session,
    *,
    period: str,
    rows_seen: int,
    revisions_seen: int,
    pages_scored: int,
    aggregate_count: int,
) -> None:
    session.execute(delete(historical_processed_periods).where(historical_processed_periods.c.period == period))
    session.execute(
        insert(historical_processed_periods).values(
            period=period,
            rows_seen=rows_seen,
            revisions_seen=revisions_seen,
            pages_scored=pages_scored,
            aggregate_count=aggregate_count,
            processed_at=datetime.now(timezone.utc),
        )
    )
    session.commit()


def historical_processed_period_exists(session: Session, period: str) -> bool:
    return (
        session.execute(
            select(historical_processed_periods.c.id)
            .where(historical_processed_periods.c.period == period)
            .limit(1)
        ).first()
        is not None
    )


def historical_aggregate_period_exists(session: Session, period: str) -> bool:
    return (
        session.execute(
            select(historical_page_aggregates.c.id)
            .where(historical_page_aggregates.c.period == period)
            .limit(1)
        ).first()
        is not None
    )


def historical_aggregate_periods(session: Session) -> list[str]:
    rows = session.execute(
        select(historical_page_aggregates.c.period)
        .distinct()
        .order_by(desc(historical_page_aggregates.c.period))
    )
    return [row[0] for row in rows]


def historical_month_periods(session: Session) -> list[str]:
    aggregate_periods = set(historical_aggregate_periods(session))
    snapshot_rows = session.execute(
        select(scoreboard_snapshots.c.period).distinct()
    )
    snapshot_periods = {row[0] for row in snapshot_rows}
    periods = [
        period
        for period in aggregate_periods | snapshot_periods
        if HISTORICAL_MONTH_RE.match(period)
    ]
    return sorted(periods, reverse=True)


def historical_year_periods_from_months(month_periods: list[str]) -> list[str]:
    years: set[str] = set()
    for period in month_periods:
        match = HISTORICAL_MONTH_RE.match(period)
        if not match:
            continue
        years.add(f"history-year:{match.group('snapshot')}:{match.group('month')[:4]}")
    return sorted(years, reverse=True)


def historical_month_periods_for_year(month_periods: list[str], year_period: str) -> list[str]:
    year_match = HISTORICAL_YEAR_RE.match(year_period)
    if not year_match:
        return []
    snapshot = year_match.group("snapshot")
    year = year_match.group("year")
    return [
        period
        for period in sorted(month_periods)
        if period.startswith(f"history:{snapshot}:{year}-")
    ]


def is_historical_year_period(period: str | None) -> bool:
    return bool(period and HISTORICAL_YEAR_RE.match(period))


def load_historical_year_aggregates(
    session: Session,
    year_period: str,
    candidate_limit: int = 200,
) -> list[dict[str, Any]]:
    month_periods = historical_month_periods_for_year(historical_month_periods(session), year_period)
    if not month_periods:
        return []

    edit_sum = func.sum(historical_page_aggregates.c.edit_count).label("edit_count")
    revert_sum = func.sum(historical_page_aggregates.c.revert_count).label("revert_count")
    mutual_sum = func.sum(historical_page_aggregates.c.mutual_revert_pairs).label("mutual_revert_pairs")
    raw_reverted_sum = func.sum(historical_page_aggregates.c.raw_reverted_count).label("raw_reverted_count")
    raw_revert_sum = func.sum(historical_page_aggregates.c.raw_revert_count).label("raw_revert_count")
    talk_edit_sum = func.sum(historical_page_aggregates.c.talk_edit_count).label("talk_edit_count")
    talk_unique_sum = func.sum(historical_page_aggregates.c.talk_unique_editors).label("talk_unique_editors")
    talk_text_max = func.max(historical_page_aggregates.c.talk_text_bytes).label("talk_text_bytes")
    candidate_rows = session.execute(
        select(
            historical_page_aggregates.c.wiki,
            historical_page_aggregates.c.page_id,
            func.max(historical_page_aggregates.c.page_title).label("page_title"),
            edit_sum,
            revert_sum,
            mutual_sum,
            raw_reverted_sum,
            raw_revert_sum,
            func.max(historical_page_aggregates.c.talk_page_id).label("talk_page_id"),
            talk_edit_sum,
            talk_unique_sum,
            talk_text_max,
            func.min(historical_page_aggregates.c.first_timestamp).label("first_timestamp"),
            func.max(historical_page_aggregates.c.last_timestamp).label("last_timestamp"),
        )
        .where(historical_page_aggregates.c.period.in_(month_periods))
        .group_by(historical_page_aggregates.c.wiki, historical_page_aggregates.c.page_id)
        .order_by(desc(mutual_sum), desc(revert_sum), desc(edit_sum))
        .limit(candidate_limit)
    ).mappings()
    candidates = [dict(row) for row in candidate_rows]
    if not candidates:
        return []

    candidate_keys = [(row["wiki"], row["page_id"]) for row in candidates]
    bucket_rows = session.execute(
        select(historical_hourly_buckets)
        .where(historical_hourly_buckets.c.period.in_(month_periods))
        .where(tuple_(historical_hourly_buckets.c.wiki, historical_hourly_buckets.c.page_id).in_(candidate_keys))
        .order_by(
            historical_hourly_buckets.c.wiki,
            historical_hourly_buckets.c.page_id,
            historical_hourly_buckets.c.bucket_start,
        )
    ).mappings()

    buckets_by_page: dict[tuple[str, int], list[dict[str, Any]]] = {}
    editors_by_page: dict[tuple[str, int], set[str]] = {}
    for bucket in bucket_rows:
        key = (bucket["wiki"], bucket["page_id"])
        editors = loads(bucket["editors_json"]) or {}
        buckets_by_page.setdefault(key, []).append(
            {
                "bucket_start": bucket["bucket_start"],
                "edit_count": bucket["edit_count"],
                "editors": editors,
                "revert_edges": loads(bucket["revert_edges_json"]) or [],
            }
        )
        editors_by_page.setdefault(key, set()).update(
            str(editor) for editor, count in editors.items() if int(count) > 0
        )

    result: list[dict[str, Any]] = []
    for candidate in candidates:
        key = (candidate["wiki"], candidate["page_id"])
        candidate["unique_editors"] = len(editors_by_page.get(key, set()))
        candidate["buckets"] = buckets_by_page.get(key, [])
        result.append(candidate)
    return result


def load_historical_year_page_stats(
    session: Session,
    year_period: str,
    candidate_limit: int = 500,
    *,
    order: str = "page-war",
) -> list[dict[str, Any]]:
    month_periods = historical_month_periods_for_year(historical_month_periods(session), year_period)
    if not month_periods:
        return []

    edit_sum = func.sum(historical_page_aggregates.c.edit_count).label("edit_count")
    unique_sum = func.sum(historical_page_aggregates.c.unique_editors).label("unique_editors")
    revert_sum = func.sum(historical_page_aggregates.c.revert_count).label("revert_count")
    mutual_sum = func.sum(historical_page_aggregates.c.mutual_revert_pairs).label("mutual_revert_pairs")
    raw_reverted_sum = func.sum(historical_page_aggregates.c.raw_reverted_count).label("raw_reverted_count")
    raw_revert_sum = func.sum(historical_page_aggregates.c.raw_revert_count).label("raw_revert_count")
    talk_edit_sum = func.sum(historical_page_aggregates.c.talk_edit_count).label("talk_edit_count")
    talk_unique_sum = func.sum(historical_page_aggregates.c.talk_unique_editors).label("talk_unique_editors")
    talk_text_max = func.max(historical_page_aggregates.c.talk_text_bytes).label("talk_text_bytes")
    raw_revert_activity = raw_reverted_sum + raw_revert_sum
    if order == "most-discussed":
        ordering = (desc(talk_text_max), desc(talk_edit_sum), desc(raw_revert_activity), desc(edit_sum))
    else:
        ordering = (desc(raw_revert_activity), desc(mutual_sum), desc(talk_edit_sum), desc(edit_sum))

    rows = session.execute(
        select(
            historical_page_aggregates.c.wiki,
            historical_page_aggregates.c.page_id,
            func.max(historical_page_aggregates.c.page_title).label("page_title"),
            edit_sum,
            unique_sum,
            revert_sum,
            mutual_sum,
            raw_reverted_sum,
            raw_revert_sum,
            func.max(historical_page_aggregates.c.talk_page_id).label("talk_page_id"),
            talk_edit_sum,
            talk_unique_sum,
            talk_text_max,
            func.min(historical_page_aggregates.c.first_timestamp).label("first_timestamp"),
            func.max(historical_page_aggregates.c.last_timestamp).label("last_timestamp"),
        )
        .where(historical_page_aggregates.c.period.in_(month_periods))
        .group_by(historical_page_aggregates.c.wiki, historical_page_aggregates.c.page_id)
        .order_by(*ordering)
        .limit(candidate_limit)
    ).mappings()
    return [dict(row) for row in rows]


def load_historical_aggregates(session: Session, period: str) -> list[dict[str, Any]]:
    bucket_rows = session.execute(
        select(historical_hourly_buckets)
        .where(historical_hourly_buckets.c.period == period)
        .order_by(
            historical_hourly_buckets.c.wiki,
            historical_hourly_buckets.c.page_id,
            historical_hourly_buckets.c.bucket_start,
        )
    ).mappings()
    buckets_by_page: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for bucket in bucket_rows:
        key = (bucket["wiki"], bucket["page_id"])
        buckets_by_page.setdefault(key, []).append(
            {
                "bucket_start": bucket["bucket_start"],
                "edit_count": bucket["edit_count"],
                "editors": loads(bucket["editors_json"]) or {},
                "revert_edges": loads(bucket["revert_edges_json"]) or [],
            }
        )

    pages = session.execute(
        select(historical_page_aggregates)
        .where(historical_page_aggregates.c.period == period)
        .order_by(historical_page_aggregates.c.wiki, historical_page_aggregates.c.page_id)
    ).mappings()
    result: list[dict[str, Any]] = []
    for page in pages:
        page_item = dict(page)
        page_item["buckets"] = buckets_by_page.get((page_item["wiki"], page_item["page_id"]), [])
        result.append(page_item)
    return result


def historical_periods(session: Session) -> list[str]:
    month_periods = historical_month_periods(session)
    year_periods = historical_year_periods_from_months(month_periods)
    return year_periods or month_periods


def historical_scoreboard(
    session: Session,
    period: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    selected_period = period
    if selected_period is None:
        selected_period = session.execute(
            select(scoreboard_snapshots.c.period)
            .distinct()
            .order_by(desc(scoreboard_snapshots.c.period))
            .limit(1)
        ).scalar_one_or_none()
    if selected_period is None:
        return []

    rows = session.execute(
        select(scoreboard_snapshots)
        .where(scoreboard_snapshots.c.period == selected_period)
        .order_by(scoreboard_snapshots.c.rank)
        .limit(limit)
    ).mappings()
    return [dict(row) for row in rows]


def save_historical_evidence(
    session: Session,
    *,
    period: str,
    wiki: str,
    page_id: int,
    page_title: str,
    source: str,
    payload: dict[str, Any],
) -> None:
    session.execute(
        delete(historical_evidence_cache).where(
            historical_evidence_cache.c.period == period,
            historical_evidence_cache.c.wiki == wiki,
            historical_evidence_cache.c.page_id == page_id,
        )
    )
    session.execute(
        insert(historical_evidence_cache).values(
            period=period,
            wiki=wiki,
            page_id=page_id,
            page_title=page_title,
            source=source,
            revision_count=int(payload.get("revision_count") or 0),
            payload_json=dumps(payload),
            generated_at=datetime.now(timezone.utc),
        )
    )
    session.commit()


def load_historical_evidence(
    session: Session,
    *,
    period: str,
    wiki: str,
    page_id: int,
) -> dict[str, Any] | None:
    row = session.execute(
        select(historical_evidence_cache)
        .where(
            historical_evidence_cache.c.period == period,
            historical_evidence_cache.c.wiki == wiki,
            historical_evidence_cache.c.page_id == page_id,
        )
        .limit(1)
    ).mappings().first()
    if row is None:
        return None
    item = dict(row)
    payload = loads(item.pop("payload_json", "{}")) or {}
    item["payload"] = payload
    return item


def apply_cached_historical_evidence(
    session: Session,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not rows:
        return rows
    keys = [
        (str(row.get("period") or ""), str(row.get("wiki") or ""), int(row.get("page_id") or 0))
        for row in rows
        if row.get("period") and row.get("wiki") and row.get("page_id")
    ]
    if not keys:
        return rows
    evidence_rows = session.execute(
        select(historical_evidence_cache).where(
            tuple_(
                historical_evidence_cache.c.period,
                historical_evidence_cache.c.wiki,
                historical_evidence_cache.c.page_id,
            ).in_(keys)
        )
    ).mappings()
    evidence_by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
    for evidence in evidence_rows:
        payload = loads(evidence["payload_json"]) or {}
        evidence_by_key[(evidence["period"], evidence["wiki"], evidence["page_id"])] = payload

    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        key = (str(item.get("period") or ""), str(item.get("wiki") or ""), int(item.get("page_id") or 0))
        payload = evidence_by_key.get(key)
        if payload:
            controversy = payload.get("controversy") or {}
            item["controversy_score"] = round(float(controversy.get("score") or 0.0), 2)
            item["battle_score"] = round(float(controversy.get("battle_score") or 0.0), 2)
            item["talk_score"] = round(float(controversy.get("talk_score") or 0.0), 2)
            item["metadata_score"] = round(float(controversy.get("metadata_score") or 0.0), 2)
            item["cleanup_penalty"] = round(float(controversy.get("cleanup_penalty") or 0.0), 2)
            item["battle_count"] = int(controversy.get("battle_count") or len(payload.get("segments") or []))
            item["talk_evidence_count"] = int(controversy.get("talk_evidence_count") or 0)
            item["local_evidence_source"] = payload.get("source") or "local_cache"
        result.append(item)
    return result


def historical_period_exists(session: Session, period: str) -> bool:
    return (
        session.execute(
            select(scoreboard_snapshots.c.id)
            .where(scoreboard_snapshots.c.period == period)
            .limit(1)
        ).first()
        is not None
    )


def _decode_window(row: dict[str, Any]) -> dict[str, Any]:
    row["features"] = loads(row.pop("feature_json", "{}")) or {}
    return row
