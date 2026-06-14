from __future__ import annotations

import asyncio
from datetime import date, datetime
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .controversy import enrich_segments_payload, rerank_historical_rows
from .ingest import run_eventstream_ingest
from .repository import (
    active_episodes,
    episode_scoreboard,
    historical_periods,
    historical_scoreboard,
    latest_window_candidates,
    recent_edits_for_page,
    recent_windows_for_page,
)
from .schema import SessionLocal, init_db
from .segments import fetch_revision_segments


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
ingest_task: asyncio.Task[None] | None = None

app = FastAPI(title="WikiWar", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def startup() -> None:
    global ingest_task
    init_db()
    if settings.start_ingest:
        ingest_task = asyncio.create_task(run_eventstream_ingest(settings))
        logger.info("Started EventStreams ingest task")


@app.on_event("shutdown")
async def shutdown() -> None:
    if ingest_task:
        ingest_task.cancel()
        try:
            await ingest_task
        except asyncio.CancelledError:
            pass


@app.get("/")
def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "ingest_enabled": settings.start_ingest,
        "wiki": settings.wiki_db,
        "server_name": settings.wiki_server_name,
        "namespace": settings.namespace,
    }


@app.get("/api/live")
def live_candidates(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    with SessionLocal() as session:
        candidates = latest_window_candidates(session, "24h", limit)
        episodes = active_episodes(session, limit)
    return {
        "candidates": serialize(candidates),
        "active_episodes": serialize(episodes),
    }


@app.get("/api/scoreboard")
def scoreboard(
    hours: int = Query(default=24, ge=1, le=24 * 30),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    with SessionLocal() as session:
        rows = episode_scoreboard(session, hours, limit)
    return {"period_hours": hours, "rows": serialize(rows)}


@app.get("/api/historical/periods")
def historical_snapshot_periods() -> dict[str, Any]:
    with SessionLocal() as session:
        periods = historical_periods(session)
    return {"periods": periods}


@app.get("/api/historical/scoreboard")
def historical_snapshot_scoreboard(
    period: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    candidate_limit = min(200, max(limit * 3, 30))
    with SessionLocal() as session:
        rows = historical_scoreboard(session, period, candidate_limit)
    selected_period = rows[0]["period"] if rows else period
    rows = rerank_historical_rows(rows, limit=limit) if rows else []
    return {"period": selected_period, "rows": serialize(rows)}


@app.get("/api/scoreboard/segments")
def scoreboard_segments(
    wiki: str,
    page_id: int,
    page_title: str = "",
    period: str | None = None,
    historical: bool = False,
) -> dict[str, Any]:
    try:
        payload = fetch_revision_segments(
            wiki=wiki,
            page_id=page_id,
            page_title=page_title,
            period=period,
        )
        if historical:
            payload = enrich_segments_payload(
                payload,
                wiki=wiki,
                page_title=page_title,
                period=period,
            )
        return serialize(payload)
    except Exception as exc:  # pragma: no cover - network/API failures should degrade in the UI.
        logger.warning("Failed to fetch contested segments for %s:%s: %s", wiki, page_id, exc)
        return {"source": "unavailable", "revision_count": 0, "segments": []}


@app.get("/api/pages/{wiki}/{page_id}")
def page_detail(wiki: str, page_id: int) -> dict[str, Any]:
    with SessionLocal() as session:
        edits = recent_edits_for_page(session, wiki, page_id)
        windows = recent_windows_for_page(session, wiki, page_id)
    latest = windows[-1] if windows else None
    return {
        "page": serialize(latest),
        "windows": serialize(windows),
        "edits": serialize(edits),
        "links": page_links(latest or (edits[0] if edits else {"page_title": "", "page_id": page_id})),
    }


@app.get("/api/events")
async def live_events() -> StreamingResponse:
    async def stream() -> Any:
        while True:
            with SessionLocal() as session:
                payload = {"candidates": serialize(latest_window_candidates(session, "24h", 50))}
            yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(settings.poll_seconds)

    return StreamingResponse(stream(), media_type="text/event-stream")


def page_links(row: dict[str, Any]) -> dict[str, str]:
    title = str(row.get("page_title") or "").replace(" ", "_")
    rev_id = row.get("rev_id")
    return {
        "article": f"https://en.wikipedia.org/wiki/{title}",
        "history": f"https://en.wikipedia.org/w/index.php?title={title}&action=history",
        "talk": f"https://en.wikipedia.org/wiki/Talk:{title}",
        "diff": f"https://en.wikipedia.org/w/index.php?diff={rev_id}" if rev_id else "",
    }


def serialize(value: Any) -> Any:
    if isinstance(value, list):
        return [serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize(item) for key, item in value.items()}
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value
