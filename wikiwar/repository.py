from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, desc, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .domain import EditEvent, RevertSignal, WindowScore
from .schema import (
    dumps,
    edits,
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
    rows = session.execute(
        select(scoreboard_snapshots.c.period).distinct().order_by(desc(scoreboard_snapshots.c.period))
    )
    return [row[0] for row in rows]


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
