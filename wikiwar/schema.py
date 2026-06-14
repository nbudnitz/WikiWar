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
    inspect,
    text,
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
    Column("raw_reverted_count", Integer, nullable=False, default=0),
    Column("raw_revert_count", Integer, nullable=False, default=0),
    Column("talk_page_id", Integer, nullable=True),
    Column("talk_edit_count", Integer, nullable=False, default=0),
    Column("talk_unique_editors", Integer, nullable=False, default=0),
    Column("talk_text_bytes", Integer, nullable=False, default=0),
    Column("talk_rfc_count", Integer, nullable=False, default=0),
    Column("talk_arbitration_count", Integer, nullable=False, default=0),
    Column("talk_restriction_count", Integer, nullable=False, default=0),
    Column("first_timestamp", DateTime(timezone=True), nullable=True),
    Column("last_timestamp", DateTime(timezone=True), nullable=True),
    UniqueConstraint("period", "wiki", "page_id", name="uq_hist_page_aggregate_period_page"),
)


page_admin_signals = Table(
    "page_admin_signals",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("restriction_count", Integer, nullable=False, default=0),
    Column("restriction_types", Text, nullable=False, default=""),
    Column("restriction_levels", Text, nullable=False, default=""),
    Column("has_extendedconfirmed", Boolean, nullable=False, default=False),
    Column("has_sysop", Boolean, nullable=False, default=False),
    Column("has_cascade", Boolean, nullable=False, default=False),
    Column("restriction_expiry", String(64), nullable=False, default=""),
    Column("protection_event_count", Integer, nullable=False, default=0),
    Column("protection_days", Float, nullable=False, default=0.0),
    Column("source", String(128), nullable=False, default=""),
    Column("imported_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("wiki", "page_id", name="uq_page_admin_signals_wiki_page"),
)


page_admin_title_signals = Table(
    "page_admin_title_signals",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("wiki", String(64), nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("protection_event_count", Integer, nullable=False, default=0),
    Column("protection_days", Float, nullable=False, default=0.0),
    Column("first_protection_at", DateTime(timezone=True), nullable=True),
    Column("last_protection_at", DateTime(timezone=True), nullable=True),
    Column("source", String(128), nullable=False, default=""),
    Column("imported_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("wiki", "page_title", name="uq_page_admin_title_signals_wiki_title"),
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


historical_evidence_cache = Table(
    "historical_evidence_cache",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("period", String(64), nullable=False),
    Column("wiki", String(64), nullable=False),
    Column("page_id", Integer, nullable=False),
    Column("page_title", String(1024), nullable=False),
    Column("source", String(64), nullable=False),
    Column("revision_count", Integer, nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("period", "wiki", "page_id", name="uq_hist_evidence_period_page"),
)


Index("ix_edits_page_time", edits.c.wiki, edits.c.page_id, edits.c.timestamp)
Index("ix_page_windows_latest", page_windows.c.wiki, page_windows.c.window_size, page_windows.c.id)
Index("ix_war_episodes_active", war_episodes.c.wiki, war_episodes.c.status, war_episodes.c.peak_score)
Index("ix_hist_page_period", historical_page_aggregates.c.period, historical_page_aggregates.c.wiki, historical_page_aggregates.c.page_id)
Index("ix_hist_bucket_period_page", historical_hourly_buckets.c.period, historical_hourly_buckets.c.wiki, historical_hourly_buckets.c.page_id, historical_hourly_buckets.c.bucket_start)
Index("ix_hist_processed_period", historical_processed_periods.c.period)
Index("ix_hist_evidence_period_page", historical_evidence_cache.c.period, historical_evidence_cache.c.wiki, historical_evidence_cache.c.page_id)
Index("ix_page_admin_signals_page", page_admin_signals.c.wiki, page_admin_signals.c.page_id)
Index("ix_page_admin_title_signals_title", page_admin_title_signals.c.wiki, page_admin_title_signals.c.page_title)


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
    target = bind or engine
    metadata.create_all(target)
    migrate_sqlite_schema(target)


def migrate_sqlite_schema(bind: Engine) -> None:
    if bind.dialect.name != "sqlite":
        return
    inspector = inspect(bind)
    if "historical_page_aggregates" not in inspector.get_table_names():
        return
    existing = {column["name"] for column in inspector.get_columns("historical_page_aggregates")}
    migrations = {
        "raw_reverted_count": "ALTER TABLE historical_page_aggregates ADD COLUMN raw_reverted_count INTEGER NOT NULL DEFAULT 0",
        "raw_revert_count": "ALTER TABLE historical_page_aggregates ADD COLUMN raw_revert_count INTEGER NOT NULL DEFAULT 0",
        "talk_page_id": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_page_id INTEGER",
        "talk_edit_count": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_edit_count INTEGER NOT NULL DEFAULT 0",
        "talk_unique_editors": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_unique_editors INTEGER NOT NULL DEFAULT 0",
        "talk_text_bytes": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_text_bytes INTEGER NOT NULL DEFAULT 0",
        "talk_rfc_count": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_rfc_count INTEGER NOT NULL DEFAULT 0",
        "talk_arbitration_count": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_arbitration_count INTEGER NOT NULL DEFAULT 0",
        "talk_restriction_count": "ALTER TABLE historical_page_aggregates ADD COLUMN talk_restriction_count INTEGER NOT NULL DEFAULT 0",
    }
    with bind.begin() as connection:
        for column, statement in migrations.items():
            if column not in existing:
                connection.execute(text(statement))
