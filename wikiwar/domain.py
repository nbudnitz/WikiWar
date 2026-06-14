from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class EditEvent:
    wiki: str
    page_id: int
    page_title: str
    namespace: int
    rev_id: int
    parent_rev_id: int | None
    timestamp: datetime
    user_id: int | None
    user_text: str
    user_is_bot: bool
    user_is_anonymous: bool
    comment: str
    tags: list[str]
    minor: bool
    old_len: int | None
    new_len: int | None
    source: str = "eventstreams"


@dataclass(frozen=True)
class RevertSignal:
    wiki: str
    page_id: int
    rev_id: int
    reverter_user: str
    reverted_user: str | None
    reverted_rev_id: int | None
    detector: str
    confidence: float
    timestamp: datetime


@dataclass(frozen=True)
class WindowScore:
    wiki: str
    page_id: int
    page_title: str
    window_start: datetime
    window_size: str
    edit_count: int
    human_edit_count: int
    unique_human_editors: int
    revert_count: int
    mutual_revert_count: int
    mutual_revert_pairs: int
    top_reverter_share: float
    revert_density: float
    edit_velocity_z: float
    conflict_score: float
    severity: str
    features: dict[str, Any]


def severity_for_score(score: float) -> str:
    if score >= 90:
        return "major"
    if score >= 75:
        return "war"
    if score >= 60:
        return "skirmish"
    if score >= 40:
        return "watch"
    return "quiet"

