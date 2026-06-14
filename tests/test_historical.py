from __future__ import annotations

import bz2
from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from wikiwar.historical import (
    EVENT_ENTITY,
    EVENT_TIMESTAMP,
    EVENT_TYPE,
    EVENT_USER_IS_BOT_BY,
    EVENT_USER_TEXT,
    PAGE_ID,
    PAGE_NAMESPACE_HISTORICAL,
    PAGE_TITLE,
    REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID,
    REVISION_ID,
    REVISION_IS_IDENTITY_REVERT,
    REVISION_IS_IDENTITY_REVERTED,
    WIKI_DB,
    PageHistoricalAggregate,
    aggregate_to_record,
    candidate_row_passes_min_score,
    historical_year_most_discussed_scoreboard,
    historical_year_page_war_scoreboard,
    dominant_reverted_user,
    effective_revert_edges,
    parse_history_row,
    process_history_files,
    probable_revert_only_cleanup_row,
    rank_historical_rows,
    record_to_aggregate,
    routine_update_penalty,
    score_historical_page,
)
from wikiwar.repository import (
    historical_month_periods,
    historical_periods,
    historical_year_periods_from_months,
    load_historical_year_aggregates,
    replace_historical_aggregates,
)
from wikiwar.schema import metadata


def test_parse_history_row_reads_revision_create() -> None:
    row = history_row(rev_id=10, user="Alice")

    revision = parse_history_row(row)

    assert revision is not None
    assert revision.wiki == "enwiki"
    assert revision.page_id == 42
    assert revision.page_title == "Example"
    assert revision.rev_id == 10
    assert revision.user_text == "Alice"


def test_parse_history_row_skips_bots() -> None:
    row = history_row(rev_id=10, user="BotUser")
    row[EVENT_USER_IS_BOT_BY] = "group"

    assert parse_history_row(row) is None


def test_score_historical_page_counts_mutual_reverts() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    now = datetime.now(timezone.utc)
    for index, user in enumerate(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"]):
        revision = parse_history_row(
            history_row(
                rev_id=100 + index,
                user=user,
                timestamp=now + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    for index in range(4):
        aggregate.add_revert("Alice", "Bob", now + timedelta(minutes=5 + index))
        aggregate.add_revert("Bob", "Alice", now + timedelta(minutes=5 + index))

    row = score_historical_page(aggregate)

    assert row["edit_count"] == 8
    assert row["unique_editors"] == 8
    assert row["revert_count"] == 8
    assert row["mutual_revert_pairs"] == 1
    assert row["peak_score"] >= 40
    assert row["war_minutes"] == 60


def test_score_historical_page_ignores_low_count_revert_edges() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    now = datetime.now(timezone.utc)
    for index, user in enumerate(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]):
        revision = parse_history_row(
            history_row(
                rev_id=400 + index,
                user=user,
                timestamp=now + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    aggregate.add_revert("Alice", "Bob", now + timedelta(minutes=2))
    aggregate.add_revert("Carol", "Dave", now + timedelta(minutes=3))
    aggregate.add_revert("Eve", "Frank", now + timedelta(minutes=4))

    row = score_historical_page(aggregate)

    assert row["revert_count"] == 0
    assert row["peak_score"] < 40


def test_score_historical_page_requires_bidirectional_reverts() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    now = datetime.now(timezone.utc)
    for index, user in enumerate(["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"]):
        revision = parse_history_row(
            history_row(
                rev_id=600 + index,
                user=user,
                timestamp=now + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    for index in range(8):
        aggregate.add_revert("Alice", "Bob", now + timedelta(minutes=index))

    row = score_historical_page(aggregate)

    assert row["revert_count"] == 0
    assert row["mutual_revert_pairs"] == 0
    assert row["peak_score"] == 0
    assert row["war_minutes"] == 0


def test_score_historical_page_can_exceed_100_for_heavy_revert_wars() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    now = datetime.now(timezone.utc)
    users = ["Alice", "Bob", "Carol", "Dave"]
    for index in range(24):
        revision = parse_history_row(
            history_row(
                rev_id=500 + index,
                user=users[index % len(users)],
                timestamp=now + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    for index in range(14):
        if index % 2 == 0:
            aggregate.add_revert("Alice", "Bob", now + timedelta(minutes=index))
        else:
            aggregate.add_revert("Bob", "Alice", now + timedelta(minutes=index))

    row = score_historical_page(aggregate)

    assert row["peak_score"] > 100


def test_score_historical_page_prefers_concentrated_back_and_forth_over_broad_churn() -> None:
    concentrated = historical_aggregate_with_activity(
        page_id=1,
        title="Concentrated",
        edit_count=34,
        editor_count=4,
        edge_counts={
            ("K6ka", "12.27.243.245"): 10,
            ("12.27.243.245", "K6ka"): 9,
            ("Satellizer", "12.27.243.245"): 5,
            ("12.27.243.245", "Satellizer"): 5,
        },
    )
    broad = historical_aggregate_with_activity(
        page_id=2,
        title="Broad",
        edit_count=70,
        editor_count=24,
        edge_counts={
            ("K6ka", "12.27.243.245"): 14,
            ("12.27.243.245", "K6ka"): 13,
            ("Satellizer", "12.27.243.245"): 8,
            ("12.27.243.245", "Satellizer"): 8,
        },
    )

    concentrated_row = score_historical_page(concentrated)
    broad_row = score_historical_page(broad)

    assert concentrated_row["peak_score"] > broad_row["peak_score"]


def test_score_historical_page_does_not_treat_sparse_month_as_continuous_war() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for day in range(30):
        revision = parse_history_row(
            history_row(
                rev_id=100 + day,
                user=f"Editor{day % 6}",
                timestamp=start + timedelta(days=day),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)

    row = score_historical_page(aggregate)

    assert row["peak_score"] < 60
    assert row["war_minutes"] == 0
    assert row["episode_count"] == 0


def test_score_historical_page_does_not_promote_volume_without_reverts() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for index in range(120):
        revision = parse_history_row(
            history_row(
                rev_id=200 + index,
                user=f"Editor{index % 15}",
                timestamp=start + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)

    row = score_historical_page(aggregate)

    assert row["revert_count"] == 0
    assert row["peak_score"] < 40
    assert row["war_minutes"] == 0


def test_routine_update_penalty_downranks_list_like_churn() -> None:
    routine = historical_aggregate_with_activity(
        page_id=1,
        title="List of Example champions",
        edit_count=90,
        editor_count=5,
        edge_counts={("Alice", "Bob"): 8, ("Bob", "Alice"): 4},
    )
    dispute = historical_aggregate_with_activity(
        page_id=2,
        title="National identity dispute",
        edit_count=32,
        editor_count=8,
        edge_counts={("Alice", "Bob"): 6, ("Bob", "Alice"): 6, ("Carol", "Dave"): 4, ("Dave", "Carol"): 4},
    )

    routine_row = score_historical_page(routine)
    dispute_row = score_historical_page(dispute)

    assert routine_update_penalty(routine_row) > routine_update_penalty(dispute_row)
    assert routine_row["candidate_score"] < routine_row["peak_score"]
    assert dispute_row["candidate_score"] == dispute_row["peak_score"]


def test_candidate_ranking_uses_adjusted_candidate_score() -> None:
    routine = {
        "peak_score": 100.0,
        "candidate_score": 46.0,
        "mutual_revert_pairs": 1,
        "revert_count": 12,
    }
    semantic = {
        "peak_score": 80.0,
        "candidate_score": 80.0,
        "mutual_revert_pairs": 2,
        "revert_count": 16,
    }

    assert rank_historical_rows([routine, semantic], 2)[0] is semantic
    assert not candidate_row_passes_min_score(routine, 60)
    assert candidate_row_passes_min_score(semantic, 60)


def test_page_war_method_promotes_raw_revert_activity_without_mutual_edges() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Gaza_war")
    now = datetime(2023, 10, 7, tzinfo=timezone.utc)
    for index in range(80):
        revision = parse_history_row(
            history_row(
                rev_id=1000 + index,
                user=f"Editor{index % 14}",
                timestamp=now + timedelta(minutes=index),
                is_reverted=index < 24,
                is_revert=index % 5 == 0,
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)

    row = score_historical_page(aggregate)
    record = aggregate_to_record(aggregate)

    assert row["peak_score"] == 0
    assert record is not None
    assert record["raw_reverted_count"] == 24
    assert record["raw_revert_count"] == 16


def test_year_methods_rank_page_war_and_discussion(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'methods.db'}", future=True)
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    article = historical_aggregate_with_activity(
        page_id=1,
        title="Gaza_war",
        edit_count=120,
        editor_count=20,
        edge_counts={},
    )
    article.page_title = "Gaza_war"
    article.raw_reverted_count = 32
    article.raw_revert_count = 21
    article.talk_page_id = 101
    article.talk_edit_count = 90
    article.talk_unique_editor_count = 30
    article.talk_text_bytes = 480_000
    pair_fight = historical_aggregate_with_activity(
        page_id=2,
        title="Pair_fight",
        edit_count=30,
        editor_count=4,
        edge_counts={("Alice", "Bob"): 4, ("Bob", "Alice"): 4},
    )
    pair_fight.page_title = "Pair_fight"
    article_record = aggregate_to_record(article)
    pair_fight_record = aggregate_to_record(pair_fight)
    assert article_record is not None
    assert pair_fight_record is not None

    with Session() as session:
        replace_historical_aggregates(session, "history:2026-05:2023-10", [article_record, pair_fight_record])

        page_war = historical_year_page_war_scoreboard(
            session,
            period="history-year:2026-05:2023",
            limit=2,
            min_score=0,
        )
        most_discussed = historical_year_most_discussed_scoreboard(
            session,
            period="history-year:2026-05:2023",
            limit=2,
        )

    assert page_war[0]["page_title"] == "Gaza_war"
    assert most_discussed[0]["page_title"] == "Gaza_war"


def test_process_history_files_reconstructs_revert_edges(tmp_path) -> None:
    path = tmp_path / "sample.tsv.bz2"
    rows = [
        history_row(rev_id=1, user="Alice", is_reverted=True, reverting_rev_id=4),
        history_row(rev_id=2, user="Carol"),
        history_row(rev_id=3, user="Dave"),
        history_row(rev_id=4, user="Bob", is_revert=True, is_reverted=True, reverting_rev_id=7),
        history_row(rev_id=5, user="Eve"),
        history_row(rev_id=6, user="Frank"),
        history_row(rev_id=7, user="Alice", is_revert=True),
        history_row(rev_id=8, user="Grace"),
    ]
    with bz2.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write("\t".join(row) + "\n")

    result = process_history_files([path], period="test-period", min_score=0, write=False)

    assert result.rows_seen == 8
    assert result.revisions_seen == 8
    assert result.pages_scored == 1
    assert result.rows_written == 0


def test_multi_revision_revert_uses_one_dominant_reverted_user() -> None:
    revisions = [
        parse_history_row(history_row(rev_id=1, user="Alice", is_reverted=True, reverting_rev_id=5)),
        parse_history_row(history_row(rev_id=2, user="Alice", is_reverted=True, reverting_rev_id=5)),
        parse_history_row(history_row(rev_id=3, user="Bob", is_reverted=True, reverting_rev_id=5)),
    ]

    assert all(revision is not None for revision in revisions)
    assert dominant_reverted_user(revisions, page_id=42) == "Alice"


def test_effective_revert_edges_require_repeated_conflict() -> None:
    edges = effective_revert_edges(
        Counter({
            ("Alice", "Bob"): 4,
            ("Carol", "Dave"): 3,
        })
    )

    assert edges == {("Alice", "Bob"): 4}


def test_historical_aggregate_record_round_trips_for_rescoring() -> None:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=42, page_title="Example")
    now = datetime.now(timezone.utc)
    for index, user in enumerate(["Alice", "Bob", "Alice", "Bob"]):
        revision = parse_history_row(
            history_row(
                rev_id=300 + index,
                user=user,
                timestamp=now + timedelta(minutes=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    for index in range(4):
        aggregate.add_revert("Alice", "Bob", now + timedelta(minutes=2 + index))
        aggregate.add_revert("Bob", "Alice", now + timedelta(minutes=3 + index))

    record = aggregate_to_record(aggregate)
    assert record is not None
    restored = record_to_aggregate(record)

    assert score_historical_page(restored) == score_historical_page(aggregate)


def test_historical_year_periods_group_completed_month_partitions() -> None:
    periods = historical_year_periods_from_months(
        [
            "history:2026-05:2014-01",
            "history:2026-05:2014-02",
            "history:2026-05:2015-01",
            "not-history",
        ]
    )

    assert periods == ["history-year:2026-05:2015", "history-year:2026-05:2014"]


def test_historical_year_aggregate_includes_newly_completed_month() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    january = aggregate_to_record(
        historical_aggregate_with_activity(
            page_id=42,
            title="Example",
            edit_count=12,
            editor_count=4,
            edge_counts={("Alice", "Bob"): 4, ("Bob", "Alice"): 4},
            start=datetime(2014, 1, 1, tzinfo=timezone.utc),
        )
    )
    february = aggregate_to_record(
        historical_aggregate_with_activity(
            page_id=42,
            title="Example",
            edit_count=10,
            editor_count=4,
            edge_counts={("Alice", "Bob"): 4, ("Bob", "Alice"): 4},
            start=datetime(2014, 2, 1, tzinfo=timezone.utc),
        )
    )
    assert january is not None
    assert february is not None

    with Session() as session:
        replace_historical_aggregates(session, "history:2026-05:2014-01", [january])

        assert historical_month_periods(session) == ["history:2026-05:2014-01"]
        assert historical_periods(session) == ["history-year:2026-05:2014"]
        records = load_historical_year_aggregates(session, "history-year:2026-05:2014", candidate_limit=10)
        assert records[0]["edit_count"] == january["edit_count"]

        replace_historical_aggregates(session, "history:2026-05:2014-02", [february])

        records = load_historical_year_aggregates(session, "history-year:2026-05:2014", candidate_limit=10)
        assert records[0]["edit_count"] == january["edit_count"] + february["edit_count"]
        assert len(records[0]["buckets"]) == len(january["buckets"]) + len(february["buckets"])


def test_probable_revert_only_cleanup_row_flags_high_density_churn() -> None:
    row = {
        "revert_density": 0.91,
        "unique_editors": 10,
        "war_minutes": 60,
        "episode_count": 1,
        "mutual_revert_pairs": 3,
    }

    assert probable_revert_only_cleanup_row(row)


def test_probable_revert_only_cleanup_row_keeps_broader_disputes() -> None:
    row = {
        "revert_density": 0.56,
        "unique_editors": 59,
        "war_minutes": 360,
        "episode_count": 5,
        "mutual_revert_pairs": 10,
    }

    assert not probable_revert_only_cleanup_row(row)


def history_row(
    *,
    rev_id: int,
    user: str,
    timestamp: datetime | None = None,
    is_reverted: bool = False,
    reverting_rev_id: int | None = None,
    is_revert: bool = False,
) -> list[str]:
    row = [""] * 78
    row[WIKI_DB] = "enwiki"
    row[EVENT_ENTITY] = "revision"
    row[EVENT_TYPE] = "create"
    row[EVENT_TIMESTAMP] = (timestamp or datetime(2020, 1, 1, tzinfo=timezone.utc)).strftime(
        "%Y-%m-%d %H:%M:%S.0"
    )
    row[EVENT_USER_TEXT] = user
    row[PAGE_ID] = "42"
    row[PAGE_TITLE] = "Example"
    row[PAGE_NAMESPACE_HISTORICAL] = "0"
    row[REVISION_ID] = str(rev_id)
    row[REVISION_IS_IDENTITY_REVERTED] = "true" if is_reverted else "false"
    row[REVISION_FIRST_IDENTITY_REVERTING_REVISION_ID] = (
        str(reverting_rev_id) if reverting_rev_id else ""
    )
    row[REVISION_IS_IDENTITY_REVERT] = "true" if is_revert else "false"
    return row


def historical_aggregate_with_activity(
    *,
    page_id: int,
    title: str,
    edit_count: int,
    editor_count: int,
    edge_counts: dict[tuple[str, str], int],
    start: datetime | None = None,
) -> PageHistoricalAggregate:
    aggregate = PageHistoricalAggregate(wiki="enwiki", page_id=page_id, page_title=title)
    start = start or datetime(2020, 1, 1, tzinfo=timezone.utc)
    users = [f"Editor{index}" for index in range(editor_count)]
    for index in range(edit_count):
        revision = parse_history_row(
            history_row(
                rev_id=(page_id * 1000) + index,
                user=users[index % len(users)],
                timestamp=start + timedelta(seconds=index),
            )
        )
        assert revision is not None
        aggregate.add_revision(revision)
    revert_index = 0
    for (reverter, reverted), count in edge_counts.items():
        for _ in range(count):
            aggregate.add_revert(reverter, reverted, start + timedelta(seconds=revert_index))
            revert_index += 1
    return aggregate
