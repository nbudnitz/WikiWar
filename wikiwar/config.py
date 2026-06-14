from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("WIKIWAR_DATABASE_URL", "sqlite:///./data/wikiwar.db")
    wiki_server_name: str = os.getenv("WIKIWAR_SERVER_NAME", "en.wikipedia.org")
    wiki_db: str = os.getenv("WIKIWAR_DB", "enwiki")
    namespace: int = int(os.getenv("WIKIWAR_NAMESPACE", "0"))
    eventstreams_url: str = os.getenv(
        "WIKIWAR_EVENTSTREAMS_URL",
        "https://stream.wikimedia.org/v2/stream/recentchange",
    )
    user_agent: str = os.getenv(
        "WIKIWAR_USER_AGENT",
        "WikiWar/0.1 (local development; https://github.com/noahbudnitz/wikipediawar) httpx",
    )
    start_ingest: bool = os.getenv("WIKIWAR_START_INGEST", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    poll_seconds: float = float(os.getenv("WIKIWAR_STREAM_POLL_SECONDS", "5"))


settings = Settings()

