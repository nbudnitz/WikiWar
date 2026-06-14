from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from .domain import EditEvent, WindowScore, severity_for_score
from .repository import (
    fetch_page_edits,
    fetch_page_reverts,
    save_window_score,
    update_war_episode,
)


WINDOWS = {
    "5m": timedelta(minutes=5),
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
}


def score_page_after_edit(session: Session, edit: EditEvent) -> list[WindowScore]:
    now = edit.timestamp
    scores: list[WindowScore] = []
    for window_size, delta in WINDOWS.items():
        score = compute_window_score(
            wiki=edit.wiki,
            page_id=edit.page_id,
            page_title=edit.page_title,
            window_size=window_size,
            window_start=now - delta,
            edits_in_window=fetch_page_edits(session, edit.wiki, edit.page_id, now - delta),
            reverts_in_window=fetch_page_reverts(session, edit.wiki, edit.page_id, now - delta),
        )
        save_window_score(session, score)
        scores.append(score)

    daily_score = next(score for score in scores if score.window_size == "24h")
    update_war_episode(session, daily_score)
    return scores


def compute_window_score(
    *,
    wiki: str,
    page_id: int,
    page_title: str,
    window_size: str,
    window_start: datetime,
    edits_in_window: list[dict[str, Any]],
    reverts_in_window: list[dict[str, Any]],
) -> WindowScore:
    human_edits = [row for row in edits_in_window if not row.get("user_is_bot")]
    human_edit_count = len(human_edits)
    unique_editors = {row["user_text"] for row in human_edits if row.get("user_text")}
    revert_count = len(reverts_in_window)
    reverter_counts = Counter(row["reverter_user"] for row in reverts_in_window)
    top_reverter_share = (
        max(reverter_counts.values()) / revert_count if reverter_counts and revert_count else 0.0
    )
    revert_density = revert_count / human_edit_count if human_edit_count else 0.0
    mutual_pairs, mutual_count = mutual_revert_metrics(reverts_in_window)
    short_window_multiplier = {"5m": 5.0, "1h": 2.0, "24h": 1.0}.get(window_size, 1.0)
    edit_velocity_z = min(5.0, human_edit_count * short_window_multiplier / 8.0)

    velocity_points = min(10.0, human_edit_count * short_window_multiplier * 0.75)
    revert_volume_points = revert_count * short_window_multiplier * 5.0
    revert_density_points = min(25.0, revert_density * 100.0)
    mutual_points = mutual_pairs * 12.0 + mutual_count * 4.0
    participant_points = min(10.0, max(0, len(unique_editors) - 2) * 3.0)
    recency_points = min(5.0, recent_activity_points(human_edits, reverts_in_window) * 0.5)
    cleanup_penalty = 15.0 if top_reverter_share >= 0.75 and revert_count >= 3 else 0.0

    score = max(
        0.0,
        velocity_points
        + revert_volume_points
        + revert_density_points
        + mutual_points
        + participant_points
        + recency_points
        - cleanup_penalty,
    )
    features = {
        "velocity_points": round(velocity_points, 2),
        "revert_volume_points": round(revert_volume_points, 2),
        "revert_density_points": round(revert_density_points, 2),
        "mutual_revert_points": round(mutual_points, 2),
        "participant_points": round(participant_points, 2),
        "recency_points": round(recency_points, 2),
        "cleanup_penalty": round(cleanup_penalty, 2),
        "thresholds": {
            "war": {
                "human_edit_count": 8,
                "unique_human_editors": 3,
                "revert_count": 4,
                "mutual_revert_pairs": 1,
                "revert_density": 0.25,
                "top_reverter_share_lt": 0.75,
            }
        },
    }

    return WindowScore(
        wiki=wiki,
        page_id=page_id,
        page_title=page_title,
        window_start=window_start,
        window_size=window_size,
        edit_count=len(edits_in_window),
        human_edit_count=human_edit_count,
        unique_human_editors=len(unique_editors),
        revert_count=revert_count,
        mutual_revert_count=mutual_count,
        mutual_revert_pairs=mutual_pairs,
        top_reverter_share=round(top_reverter_share, 4),
        revert_density=round(revert_density, 4),
        edit_velocity_z=round(edit_velocity_z, 4),
        conflict_score=round(score, 2),
        severity=severity_for_score(score),
        features=features,
    )


def mutual_revert_metrics(reverts_in_window: list[dict[str, Any]]) -> tuple[int, int]:
    edges: Counter[tuple[str, str]] = Counter()
    for row in reverts_in_window:
        reverted_user = row.get("reverted_user")
        reverter_user = row.get("reverter_user")
        if not reverted_user or not reverter_user or reverted_user == reverter_user:
            continue
        edges[(reverter_user, reverted_user)] += 1

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


def recent_activity_points(edits_in_window: list[dict[str, Any]], reverts_in_window: list[dict[str, Any]]) -> float:
    if not edits_in_window:
        return 0.0
    latest_timestamp = max(row["timestamp"] for row in edits_in_window)
    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
    since = latest_timestamp - timedelta(minutes=5)
    recent_edits = sum(1 for row in edits_in_window if _aware(row["timestamp"]) >= since)
    recent_reverts = sum(1 for row in reverts_in_window if _aware(row["timestamp"]) >= since)
    return recent_edits + (recent_reverts * 4.0)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
