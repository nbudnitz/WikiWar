from datetime import datetime, timezone

from wikiwar.domain import EditEvent
from wikiwar.reverts import detect_revert


def test_detects_revert_from_tag_and_comment_user() -> None:
    edit = EditEvent(
        wiki="enwiki",
        page_id=1,
        page_title="Example",
        namespace=0,
        rev_id=123,
        parent_rev_id=122,
        timestamp=datetime.now(timezone.utc),
        user_id=10,
        user_text="Alice",
        user_is_bot=False,
        user_is_anonymous=False,
        comment="Reverted edits by [[Special:Contributions/Bob|Bob]] to last version by Alice",
        tags=["mw-rollback"],
        minor=False,
        old_len=100,
        new_len=90,
    )

    signal = detect_revert(edit)

    assert signal is not None
    assert signal.detector == "mw-rollback"
    assert signal.reverted_user == "Bob"
    assert signal.confidence == 0.95


def test_ignores_non_revert_edit() -> None:
    edit = EditEvent(
        wiki="enwiki",
        page_id=1,
        page_title="Example",
        namespace=0,
        rev_id=124,
        parent_rev_id=123,
        timestamp=datetime.now(timezone.utc),
        user_id=10,
        user_text="Alice",
        user_is_bot=False,
        user_is_anonymous=False,
        comment="Copyedit lead section",
        tags=[],
        minor=False,
        old_len=100,
        new_len=110,
    )

    assert detect_revert(edit) is None

