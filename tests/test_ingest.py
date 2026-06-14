from wikiwar.config import Settings
from wikiwar.ingest import is_candidate_recentchange, normalize_recentchange


def test_filters_to_enwiki_article_non_bot_edits() -> None:
    config = Settings(start_ingest=False)
    payload = {
        "server_name": "en.wikipedia.org",
        "wiki": "enwiki",
        "namespace": 0,
        "bot": False,
        "type": "edit",
        "page_id": 42,
        "title": "Example",
    }

    assert is_candidate_recentchange(payload, config)

    payload["namespace"] = 1
    assert not is_candidate_recentchange(payload, config)


def test_normalizes_recentchange_payload() -> None:
    payload = {
        "wiki": "enwiki",
        "page_id": 42,
        "title": "Example",
        "namespace": 0,
        "revision": {"new": 100, "old": 99},
        "timestamp": 1_700_000_000,
        "user": "Alice",
        "user_id": 5,
        "comment": "Copyedit",
        "tags": ["visualeditor"],
        "length": {"old": 10, "new": 20},
    }

    edit = normalize_recentchange(payload)

    assert edit is not None
    assert edit.rev_id == 100
    assert edit.parent_rev_id == 99
    assert edit.page_title == "Example"
    assert edit.tags == ["visualeditor"]
