from __future__ import annotations

import re

from .domain import EditEvent, RevertSignal


REVERT_TAGS = {
    "mw-rollback": 0.95,
    "mw-undo": 0.9,
    "mw-manual-revert": 0.85,
}

REVERT_COMMENT_RE = re.compile(r"\b(revert(?:ed|ing)?|undid|undo|rollback|rvv?|restore[ds]?)\b", re.I)
REVISION_RE = re.compile(r"\b(?:revision|rev(?:ision)?id?)\s+(\d+)\b", re.I)
BY_USER_PATTERNS = [
    re.compile(r"\bby\s+\[\[(?:Special:Contributions/)?([^|\]]+)(?:\|[^\]]+)?\]\]", re.I),
    re.compile(r"\bby\s+([^:;,.()]+?)(?:\s+\(|$)", re.I),
    re.compile(r"\bReverted edits by\s+([^:;,.()]+)", re.I),
]


def detect_revert(edit: EditEvent) -> RevertSignal | None:
    tag_hits = [(tag, REVERT_TAGS[tag]) for tag in edit.tags if tag in REVERT_TAGS]
    detector = None
    confidence = 0.0

    if tag_hits:
        detector, confidence = max(tag_hits, key=lambda item: item[1])
    elif REVERT_COMMENT_RE.search(edit.comment):
        detector = "comment"
        confidence = 0.65

    if detector is None:
        return None

    reverted_user = extract_reverted_user(edit.comment)
    reverted_rev_id = extract_reverted_revision(edit.comment)
    if reverted_user and reverted_user == edit.user_text:
        reverted_user = None

    return RevertSignal(
        wiki=edit.wiki,
        page_id=edit.page_id,
        rev_id=edit.rev_id,
        reverter_user=edit.user_text,
        reverted_user=reverted_user,
        reverted_rev_id=reverted_rev_id,
        detector=detector,
        confidence=confidence,
        timestamp=edit.timestamp,
    )


def extract_reverted_revision(comment: str) -> int | None:
    match = REVISION_RE.search(comment)
    if not match:
        return None
    return int(match.group(1))


def extract_reverted_user(comment: str) -> str | None:
    for pattern in BY_USER_PATTERNS:
        match = pattern.search(comment)
        if match:
            value = match.group(1).strip()
            return value.strip("[] ")
    return None

