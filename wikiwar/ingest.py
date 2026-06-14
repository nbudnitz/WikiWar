from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
import json
import logging
from typing import Any

import httpx

from .config import Settings, settings
from .domain import EditEvent
from .repository import save_edit, save_raw_event, save_revert
from .reverts import detect_revert
from .schema import SessionLocal
from .scoring import score_page_after_edit


logger = logging.getLogger(__name__)


async def run_eventstream_ingest(config: Settings = settings) -> None:
    backoff = 1.0
    while True:
        try:
            async for stream_id, payload in eventstream_events(config):
                await asyncio.to_thread(process_recentchange_event, stream_id, payload, config)
            backoff = 1.0
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("EventStreams ingest failed; retrying in %.1fs", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)


async def eventstream_events(config: Settings = settings) -> AsyncIterator[tuple[str | None, dict[str, Any]]]:
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": config.user_agent,
    }
    timeout = httpx.Timeout(connect=15.0, read=None, write=15.0, pool=15.0)
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        async with client.stream("GET", config.eventstreams_url) as response:
            response.raise_for_status()
            event_id: str | None = None
            data_lines: list[str] = []
            async for line in response.aiter_lines():
                if line == "":
                    if data_lines:
                        data = "\n".join(data_lines)
                        data_lines = []
                        try:
                            yield event_id, json.loads(data)
                        except json.JSONDecodeError:
                            logger.debug("Skipping non-JSON EventStreams payload")
                    event_id = None
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("id:"):
                    event_id = line.removeprefix("id:").strip()
                    continue
                if line.startswith("data:"):
                    data_lines.append(line.removeprefix("data:").strip())


def process_recentchange_event(
    stream_id: str | None,
    payload: dict[str, Any],
    config: Settings = settings,
) -> bool:
    if not is_candidate_recentchange(payload, config):
        return False

    with SessionLocal() as session:
        is_new = save_raw_event(session, stream_id, datetime.now(timezone.utc), payload)
    if not is_new:
        return False

    edit = normalize_recentchange(payload)
    if edit is None:
        return False

    with SessionLocal() as session:
        saved = save_edit(session, edit)
    if not saved:
        return False

    revert = detect_revert(edit)
    if revert:
        with SessionLocal() as session:
            save_revert(session, revert)

    with SessionLocal() as session:
        score_page_after_edit(session, edit)

    return True


def is_candidate_recentchange(payload: dict[str, Any], config: Settings = settings) -> bool:
    if payload.get("server_name") != config.wiki_server_name:
        return False
    if payload.get("wiki") != config.wiki_db:
        return False
    if payload.get("namespace") != config.namespace:
        return False
    if payload.get("bot") is True:
        return False
    if payload.get("type") not in {"edit", "new"}:
        return False
    if not payload.get("page_id") or not payload.get("title"):
        return False
    return True


def normalize_recentchange(payload: dict[str, Any]) -> EditEvent | None:
    revision = payload.get("revision") or {}
    rev_id = _int_or_none(revision.get("new") or payload.get("rev_id"))
    if rev_id is None:
        return None
    parent_rev_id = _int_or_none(revision.get("old") or payload.get("old_revid"))
    length = payload.get("length") or {}
    timestamp = payload.get("timestamp")
    if isinstance(timestamp, int | float):
        event_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    elif isinstance(timestamp, str):
        event_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    else:
        event_time = datetime.now(timezone.utc)

    user = str(payload.get("user") or "unknown")
    user_id = _int_or_none(payload.get("user_id"))
    return EditEvent(
        wiki=str(payload.get("wiki") or "enwiki"),
        page_id=int(payload.get("page_id") or 0),
        page_title=str(payload.get("title") or "Untitled"),
        namespace=int(payload.get("namespace") or 0),
        rev_id=rev_id,
        parent_rev_id=parent_rev_id,
        timestamp=event_time,
        user_id=user_id,
        user_text=user,
        user_is_bot=bool(payload.get("bot")),
        user_is_anonymous=user_id is None,
        comment=str(payload.get("comment") or ""),
        tags=[str(tag) for tag in (payload.get("tags") or [])],
        minor=bool(payload.get("minor")),
        old_len=_int_or_none(length.get("old") if isinstance(length, dict) else None),
        new_len=_int_or_none(length.get("new") if isinstance(length, dict) else None),
    )


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
