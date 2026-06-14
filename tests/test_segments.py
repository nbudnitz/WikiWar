from __future__ import annotations

from datetime import datetime, timezone

from wikiwar.segments import (
    changed_token_bounds,
    changed_text_segments,
    changed_text_snippets,
    diverse_segments,
    period_bounds,
    top_contested_revision_pairs,
    top_contested_text_segments,
)


def test_changed_text_snippets_extracts_actual_changed_phrase() -> None:
    snippets = changed_text_snippets(
        "The policy says the claim is disputed by researchers.",
        "The policy says the claim is strongly disputed by researchers.",
    )

    assert "strongly" in snippets


def test_changed_text_segments_include_context() -> None:
    segments = changed_text_segments(
        "The policy says the claim is disputed by researchers.",
        "The policy says the claim is strongly disputed by researchers.",
    )

    assert segments[0]["segment"] == "strongly"
    assert segments[0]["context_before"] == "The policy says the claim is"
    assert segments[0]["context_after"] == "disputed by researchers"


def test_changed_text_segments_preserve_display_punctuation() -> None:
    segments = changed_text_segments(
        "older people. The younger generations is in its majority Spanish-speaking. The territory maintains an identity.",
        "older people. The younger generations is Spanish-speaking. The territory maintains an identity.",
    )

    assert segments[0]["segment"] == "in its majority"
    assert segments[0]["context_before"] == "older people. The younger generations is"
    assert segments[0]["context_after"] == "Spanish-speaking. The territory maintains an identity"


def test_changed_text_segments_expand_title_phrase_but_keep_exact_changed_text() -> None:
    segments = changed_text_segments(
        "8 million live in Republic of Azerbaijan. There are also sizeable communities.",
        "8 million live in Republic of. There are also sizeable communities.",
    )

    assert segments[0]["segment"] == "Republic of Azerbaijan"
    assert segments[0]["changed_text"] == "Azerbaijan"
    assert segments[0]["highlight_before"] == "Republic of"
    assert segments[0]["context_before"] == "8 million live in"
    assert segments[0]["context_after"] == "There are also sizeable communities"


def test_changed_text_segments_classify_word_swap() -> None:
    segments = changed_text_segments(
        "The article called him a leader of the party.",
        "The article called him a dictator of the party.",
    )

    assert len(segments) == 1
    assert segments[0]["change_type"] == "swap"
    assert segments[0]["segment"] == "dictator"
    assert segments[0]["swap_values"] == ["leader", "dictator"]


def test_changed_token_bounds_trims_unchanged_edges() -> None:
    before = ["alpha", "beta", "old", "omega"]
    after = ["alpha", "beta", "new", "omega"]

    assert changed_token_bounds(before, after) == (2, 3, 3)


def test_changed_text_segments_keep_context_after_diff_trimming() -> None:
    segments = changed_text_segments(
        "Alpha beta gamma delta epsilon zeta eta theta.",
        "Alpha beta gamma delta contested epsilon zeta eta theta.",
    )

    assert segments[0]["segment"] == "contested"
    assert segments[0]["context_before"] == "Alpha beta gamma delta"
    assert segments[0]["context_after"] == "epsilon zeta eta theta"


def test_top_contested_text_segments_weights_repeated_reverted_text() -> None:
    revisions = [
        {
            "content": "The article says the trial was fair.",
            "user_text": "Alice",
            "comment": "initial",
            "tags": [],
            "timestamp": "2020-01-01T00:00:00Z",
        },
        {
            "content": "The article says the trial was politically motivated.",
            "user_text": "Bob",
            "comment": "changed wording",
            "tags": [],
            "timestamp": "2020-01-01T00:01:00Z",
        },
        {
            "content": "The article says the trial was fair.",
            "user_text": "Alice",
            "comment": "revert",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:02:00Z",
        },
        {
            "content": "The article says the trial was politically motivated.",
            "user_text": "Carol",
            "comment": "restore wording",
            "tags": [],
            "timestamp": "2020-01-01T00:03:00Z",
        },
        {
            "content": "The article says the trial was fair.",
            "user_text": "Alice",
            "comment": "rollback",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:04:00Z",
        },
    ]

    rows = top_contested_text_segments(revisions)

    assert rows[0]["segment"] in {"politically motivated", "fair"}
    assert rows[0]["change_type"] == "swap"
    assert rows[0]["swap_values"] == ["fair", "politically motivated"]
    assert rows[0]["changes"] >= 2
    assert rows[0]["editors"] >= 2
    assert rows[0]["combatants"] >= 2


def test_top_contested_text_segments_excludes_low_signal_battles() -> None:
    revisions = [
        {
            "rev_id": 1,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Alice",
            "comment": "initial",
            "tags": [],
            "timestamp": "2020-01-01T00:00:00Z",
        },
        {
            "rev_id": 2,
            "content": "Alpha topic became heated. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Bob",
            "comment": "edit",
            "tags": [],
            "timestamp": "2020-01-01T00:01:00Z",
        },
        {
            "rev_id": 3,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Alice",
            "comment": "revert",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:02:00Z",
        },
        {
            "rev_id": 4,
            "content": "Alpha topic became heated. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Bob",
            "comment": "restore",
            "tags": [],
            "timestamp": "2020-01-01T00:03:00Z",
        },
        {
            "rev_id": 5,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Alice",
            "comment": "revert",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:04:00Z",
        },
        {
            "rev_id": 6,
            "content": "Alpha topic remained plain. Beta topic turned controversial. Gamma topic remained fixed.",
            "user_text": "Carol",
            "comment": "beta wording",
            "tags": [],
            "timestamp": "2020-01-01T00:05:00Z",
        },
        {
            "rev_id": 7,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Alice",
            "comment": "revert beta",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:06:00Z",
        },
        {
            "rev_id": 8,
            "content": "Alpha topic remained plain. Beta topic turned controversial. Gamma topic remained fixed.",
            "user_text": "Carol",
            "comment": "restore beta",
            "tags": [],
            "timestamp": "2020-01-01T00:07:00Z",
        },
        {
            "rev_id": 9,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic remained fixed.",
            "user_text": "Alice",
            "comment": "revert beta",
            "tags": ["mw-rollback"],
            "timestamp": "2020-01-01T00:08:00Z",
        },
        {
            "rev_id": 10,
            "content": "Alpha topic remained plain. Beta topic remained calm. Gamma topic became noisy.",
            "user_text": "Dave",
            "comment": "one-off",
            "tags": [],
            "timestamp": "2020-01-01T00:09:00Z",
        },
    ]

    rows = top_contested_text_segments(revisions, limit=3)
    segments = {row["segment"] for row in rows}

    assert len(rows) == 2
    assert any("heated" in segment or "plain" in segment for segment in segments)
    assert any("controversial" in segment or "calm" in segment for segment in segments)
    assert all("noisy" not in segment for segment in segments)


def test_top_contested_revision_pairs_prioritizes_short_repeated_battles_over_large_changes() -> None:
    large_before = "Lead sentence. " + " ".join(f"alpha{i}" for i in range(60))
    large_after = "Lead sentence. " + " ".join(f"beta{i}" for i in range(60))
    short_before = "The page called him a leader of the party."
    short_after = "The page called him a dictator of the party."
    revision_pairs = []
    for index in range(4):
        revision_pairs.append(
            (
                {"rev_id": 100 + index, "content": large_before, "user_text": "Previous"},
                {
                    "rev_id": 200 + index,
                    "content": large_after,
                    "user_text": f"LargeEditor{index}",
                    "comment": "revert large paragraph",
                    "tags": [],
                },
            )
        )
    for index in range(3):
        revision_pairs.append(
            (
                {"rev_id": 300 + index, "content": short_before, "user_text": "Previous"},
                {
                    "rev_id": 400 + index,
                    "content": short_after,
                    "user_text": f"ShortEditor{index}",
                    "comment": "revert wording",
                    "tags": [],
                },
            )
        )

    rows = top_contested_revision_pairs(revision_pairs, limit=2)

    assert rows[0]["change_type"] == "swap"
    assert rows[0]["changed_text"] == "dictator"
    assert rows[0]["score"] > rows[1]["score"]


def test_top_contested_revision_pairs_counts_reverted_editor_as_combatant() -> None:
    revision_pairs = []
    for index in range(3):
        revision_pairs.append(
            (
                {
                    "rev_id": 100 + index,
                    "content": "The page called him a leader of the faction.",
                    "user_text": "Alice",
                },
                {
                    "rev_id": 200 + index,
                    "content": "The page called him a dictator of the faction.",
                    "user_text": "Bob",
                    "comment": "revert wording",
                    "tags": [],
                },
            )
        )

    rows = top_contested_revision_pairs(revision_pairs, limit=1)

    assert len(rows) == 1
    assert rows[0]["combatants"] == 2
    assert rows[0]["reverts"] == 3


def test_top_contested_revision_pairs_exposes_first_shot_comment() -> None:
    revision_pairs = []
    for index in range(3):
        revision_pairs.append(
            (
                {
                    "rev_id": 100 + index,
                    "content": "The river is called Ganga in the lead.",
                    "user_text": "Alice",
                    "comment": "Use the common Indian name Ganga in the lead. This is supported by sources.",
                },
                {
                    "rev_id": 200 + index,
                    "content": "The river is called Ganges in the lead.",
                    "user_text": "Bob",
                    "comment": "revert disputed naming",
                    "tags": [],
                },
            )
        )

    rows = top_contested_revision_pairs(revision_pairs, limit=1)

    assert rows[0]["first_shot_comment"] == "Use the common Indian name Ganga in the lead."


def test_top_contested_text_segments_groups_comoving_revert_phrases() -> None:
    version_a = (
        "The genre was shaped by older fans. "
        "Critics called it metal with gothic atmosphere. "
        "Supporters said it grew through underground clubs."
    )
    version_b = (
        "The genre was shaped by newer fans. "
        "Critics called it rock with gothic atmosphere. "
        "Supporters said it grew through online forums."
    )
    revisions = []
    for index in range(10):
        revisions.append(
            {
                "rev_id": index + 1,
                "content": version_a if index % 2 == 0 else version_b,
                "user_text": f"Editor{index % 3}",
                "comment": "revert" if index >= 2 else "edit",
                "tags": [],
                "timestamp": f"2020-01-01T00:{index:02}:00Z",
            }
        )

    rows = top_contested_text_segments(revisions)
    grouped = next(row for row in rows if row.get("battle_group_size"))

    assert grouped["battle_group_size"] >= 2
    assert grouped["reverts"] >= 4
    assert grouped["changes"] >= 4
    assert len(grouped["related_segments"]) >= 2
    assert all("_revert_revision_ids" not in row for row in grouped["related_segments"])


def test_period_bounds_uses_historical_partition_month() -> None:
    start, end = period_bounds("history:2026-05:2007-09")

    assert start == datetime(2007, 9, 1, tzinfo=timezone.utc)
    assert end == datetime(2007, 10, 1, tzinfo=timezone.utc)


def test_diverse_segments_filters_overlapping_phrase_variants() -> None:
    rows = [
        {"segment": "It has been contended that bush wanted to", "score": 10},
        {"segment": "has been contended that bush wanted to put", "score": 10},
        {"segment": "military service records", "score": 8},
    ]

    selected = diverse_segments(rows, 3)

    assert [row["segment"] for row in selected] == [
        "It has been contended that bush wanted to",
        "military service records",
    ]
