from __future__ import annotations

from wikiwar.controversy import score_controversy_evidence


def test_controversy_score_requires_article_battles() -> None:
    score = score_controversy_evidence(
        peak_score=500,
        revert_count=250,
        mutual_revert_pairs=3,
        segments=[],
        talk={"score": 120, "evidence": [{"sentence": "Editors debated the wording."}]},
    )

    assert score["score"] == 0


def test_controversy_score_weights_talk_evidence_and_swaps() -> None:
    segments = [
        {
            "change_type": "swap",
            "score": 60,
            "reverts": 4,
            "changes": 5,
            "combatants": 3,
            "cleanup_comment_count": 0,
            "comment_count": 4,
        }
    ]

    without_talk = score_controversy_evidence(
        peak_score=120,
        revert_count=20,
        mutual_revert_pairs=1,
        segments=segments,
        talk={"score": 0, "evidence": []},
    )
    with_talk = score_controversy_evidence(
        peak_score=120,
        revert_count=20,
        mutual_revert_pairs=1,
        segments=segments,
        talk={"score": 80, "evidence": [{"sentence": "Editors debated the disputed wording."}]},
    )

    assert with_talk["score"] > without_talk["score"] + 80


def test_controversy_score_penalizes_cleanup_without_talk() -> None:
    segments = [
        {
            "change_type": "deletion",
            "score": 80,
            "reverts": 6,
            "changes": 6,
            "combatants": 2,
            "cleanup_comment_count": 5,
            "comment_count": 6,
        }
    ]

    score = score_controversy_evidence(
        peak_score=300,
        revert_count=80,
        mutual_revert_pairs=2,
        segments=segments,
        talk={"score": 0, "evidence": []},
    )

    assert score["cleanup_penalty"] > 0
    assert score["score"] < score["battle_score"]
    assert score["score"] <= 25
