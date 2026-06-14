from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from .config import settings


metadata = MetaData()


raw_events = Table(
    "raw_events",
    metadata,
    # stream_id is not globally guaranteed by SQLAlchemy across local tests, so keep a surrogate key.
    # The unique constraint still prevents replay duplicates when EventStreams sends ids.
    Column("id", Integer, primary_key=True),
    Column("stream_id", String(255), nullable=True),
    Column("received_at", DateTime(timezone=True), nullable=False),
    Column("payload_json", Text, nullable=False),
    UniqueConstraint("stream_id", name="uq_raw_events_stream_id"),
)


edits = Table(
    "edits",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("namespace", Integer, nullable=False),
    Column("rev_id", Integer, nullable=False),
    Column("parent_rev_id", Integer, nullable=True),
    Column("timestamp", DateTime(timezone=True), nullable=False),
    Column("user_id", Integer, nullable=True),
    Column("user_text", String(512), nullable=False),
    Column("user_is_bot", Boolean, nullable=False, default=False),
    Column("user_is_anonymous", Boolean, nullable=False, default=False),
    Column("comment", Text, nullable=False, default=""),
    Column("tags", Text, nullable=False, default="[]"),
    Column("minor", Boolean, nullable=False, default=False),
    Column("old_len", Integer, nullable=True),
    Column("new_len", Integer, nullable=True),
    Column("source", String(64), nullable=False, default="eventstreams"),
    UniqueConstraint("wiki", "rev_id", name="uq_edits_wiki_rev"),
)


reverts = Table(
    "reverts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("rev_id", Integer, nullable=False),
    Column("reverter_user", String(512), nullable=False),
    Column("reverted_user", String(512), nullable=True),
    Column("reverted_rev_id", Integer, nullable=True),
    Column("detector", String(64), nullable=False),
    Column("confidence", Float, nullable=False),
    Column("timestamp", DateTime(timezone=True), nullable=False),
    UniqueConstraint("wiki", "rev_id", name="uq_reverts_wiki_rev"),
)


page_windows = Table(
    "page_windows",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("window_size", String(16), nullable=False),
    Column("edit_count", Integer, nullable=False),
    Column("human_edit_count", Integer, nullable=False),
    Column("unique_human_editors", Integer, nullable=False),
    Column("revert_count", Integer, nullable=False),
    Column("mutual_revert_count", Integer, nullable=False),
    Column("mutual_revert_pairs", Integer, nullable=False),
    Column("top_reverter_share", Float, nullable=False),
    Column("revert_density", Float, nullable=False),
    Column("edit_velocity_z", Float, nullable=False),
    Column("conflict_score", Float, nullable=False),
    Column("severity", String(32), nullable=False),
    Column("feature_json", Text, nullable=False),
)


war_episodes = Table(
    "war_episodes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("ended_at", DateTime(timezone=True), nullable=True),
    Column("peak_score", Float, nullable=False),
    Column("score_area", Float, nullable=False),
    Column("total_edits", Integer, nullable=False),
    Column("total_reverts", Integer, nullable=False),
    Column("total_mutual_reverts", Integer, nullable=False),
    Column("participants", Integer, nullable=False),
    Column("status", String(32), nullable=False),
)


scoreboard_snapshots = Table(
    "scoreboard_snapshots",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("period", String(32), nullable=False),
    Column("rank", Integer, nullable=False),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("score_area", Float, nullable=False),
    Column("peak_score", Float, nullable=False),
    Column("war_minutes", Integer, nullable=False),
    Column("episode_count", Integer, nullable=False),
)


historical_page_aggregates = Table(
    "historical_page_aggregates",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("period", String(32), nullable=False),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("edit_count", Integer, nullable=False),
    Column("unique_editors", Integer, nullable=False),
    Column("revert_count", Integer, nullable=False),
    Column("mutual_revert_pairs", Integer, nullable=False),
    Column("first_timestamp", DateTime(timezone=True), nullable=True),
    Column("last_timestamp", DateTime(timezone=True), nullable=True),
    UniqueConstraint("period", "wiki", "page_id", name="uq_hist_page_aggregate_period_page"),
)


historical_hourly_buckets = Table(
    "historical_hourly_buckets",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("period", String(32), nullable=False),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("bucket_start", DateTime(timezone=True), nullable=False),
    Column("edit_count", Integer, nullable=False),
    Column("editors_json", Text, nullable=False),
    Column("revert_edges_json", Text, nullable=False),
    UniqueConstraint("period", "wiki", "page_id", "bucket_start", name="uq_hist_bucket_period_page_hour"),
)


historical_processed_periods = Table(
    "historical_processed_periods",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("period", String(64), nullable=False),
    Column("rows_seen", Integer, nullable=False),
    Column("revisions_seen", Integer, nullable=False),
    Column("pages_scored", Integer, nullable=False),
    Column("aggregate_count", Integer, nullable=False),
    Column("processed_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("period", name="uq_hist_processed_period"),
)


Index("ix_edits_page_time", edits.c.wiki, edits.c.page_id, edits.c.timestamp)
Index("ix_page_windows_latest", page_windows.c.wiki, page_windows.c.window_size, page_windows.c.id)
Index("ix_war_episodes_active", war_episodes.c.wiki, war_episodes.c.status, war_episodes.c.peak_score)
Index("ix_hist_page_period", historical_page_aggregates.c.period, historical_page_aggregates.c.wiki, historical_page_aggregates.c.page_id)
Index("ix_hist_bucket_period_page", historical_hourly_buckets.c.period, historical_hourly_buckets.c.wiki, historical_hourly_buckets.c.page_id, historical_hourly_buckets.c.bucket_start)
Index("ix_hist_processed_period", historical_processed_periods.c.period)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dumps(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def loads(value: str | None) -> object:
    if not value:
        return None
    return json.loads(value)


def build_engine(database_url: str | None = None) -> Engine:
    url = database_url or settings.database_url
    if url.startswith("sqlite:///"):
        db_path = Path(url.removeprefix("sqlite:///"))
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
        return create_engine(url, connect_args={"check_same_thread": False, "timeout": 120}, future=True)
    return create_engine(url, pool_pre_ping=True, future=True)


engine = build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def init_db(bind: Engine | None = None) -> None:
    metadata.create_all(bind or engine)
