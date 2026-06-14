from datetime import datetime, timedelta, timezone

from wikiwar.scoring import compute_window_score, mutual_revert_metrics


def test_mutual_revert_metrics_count_balanced_pairs() -> None:
    rows = [
        {"reverter_user": "Alice", "reverted_user": "Bob"},
        {"reverter_user": "Alice", "reverted_user": "Bob"},
        {"reverter_user": "Bob", "reverted_user": "Alice"},
        {"reverter_user": "Carol", "reverted_user": "Dave"},
    ]

    assert mutual_revert_metrics(rows) == (1, 1)


def test_compute_window_score_promotes_mutual_reverts() -> None:
    now = datetime.now(timezone.utc)
    edits = [
        {"user_text": "Alice", "user_is_bot": False, "timestamp": now - timedelta(minutes=8)},
        {"user_text": "Bob", "user_is_bot": False, "timestamp": now - timedelta(minutes=7)},
        {"user_text": "Carol", "user_is_bot": False, "timestamp": now - timedelta(minutes=6)},
        {"user_text": "Alice", "user_is_bot": False, "timestamp": now - timedelta(minutes=5)},
        {"user_text": "Bob", "user_is_bot": False, "timestamp": now - timedelta(minutes=4)},
        {"user_text": "Carol", "user_is_bot": False, "timestamp": now - timedelta(minutes=3)},
        {"user_text": "Alice", "user_is_bot": False, "timestamp": now - timedelta(minutes=2)},
        {"user_text": "Bob", "user_is_bot": False, "timestamp": now - timedelta(minutes=1)},
    ]
    reverts = [
        {"reverter_user": "Alice", "reverted_user": "Bob", "timestamp": now - timedelta(minutes=5)},
        {"reverter_user": "Bob", "reverted_user": "Alice", "timestamp": now - timedelta(minutes=4)},
        {"reverter_user": "Carol", "reverted_user": "Bob", "timestamp": now - timedelta(minutes=3)},
        {"reverter_user": "Alice", "reverted_user": "Carol", "timestamp": now - timedelta(minutes=2)},
    ]

    score = compute_window_score(
        wiki="enwiki",
        page_id=1,
        page_title="Example",
        window_size="24h",
        window_start=now - timedelta(hours=24),
        edits_in_window=edits,
        reverts_in_window=reverts,
    )

    assert score.human_edit_count == 8
    assert score.unique_human_editors == 3
    assert score.revert_count == 4
    assert score.mutual_revert_pairs == 1
    assert score.revert_density == 0.5
    assert score.conflict_score >= 60


def test_compute_window_score_can_exceed_100_for_heavy_revert_wars() -> None:
    now = datetime.now(timezone.utc)
    users = ["Alice", "Bob", "Carol", "Dave"]
    edits = [
        {
            "user_text": users[index % len(users)],
            "user_is_bot": False,
            "timestamp": now - timedelta(minutes=20 - index),
        }
        for index in range(20)
    ]
    reverts = [
        {
            "reverter_user": "Alice" if index % 2 == 0 else "Bob",
            "reverted_user": "Bob" if index % 2 == 0 else "Alice",
            "timestamp": now - timedelta(minutes=10 - (index % 10)),
        }
        for index in range(12)
    ]

    score = compute_window_score(
        wiki="enwiki",
        page_id=1,
        page_title="Example",
        window_size="24h",
        window_start=now - timedelta(hours=24),
        edits_in_window=edits,
        reverts_in_window=reverts,
    )

    assert score.conflict_score > 100


def test_compute_window_score_does_not_promote_volume_without_reverts() -> None:
    now = datetime.now(timezone.utc)
    edits = [
        {
            "user_text": f"Editor{index % 12}",
            "user_is_bot": False,
            "timestamp": now - timedelta(minutes=index),
        }
        for index in range(60)
    ]

    score = compute_window_score(
        wiki="enwiki",
        page_id=1,
        page_title="Example",
        window_size="24h",
        window_start=now - timedelta(hours=24),
        edits_in_window=edits,
        reverts_in_window=[],
    )

    assert score.human_edit_count == 60
    assert score.revert_count == 0
    assert score.conflict_score < 40
