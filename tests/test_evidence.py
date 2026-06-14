from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from wikiwar import app as wikiwar_app
from wikiwar.controversy import build_local_evidence_payload
from wikiwar.evidence import CandidateEvidenceInput, collect_revisions_from_xml_dumps
from wikiwar.evidence import (
    RevisionDumpShard,
    read_evidence_status,
    revision_shards_for_page_ids,
    talk_page_ids_from_history_dumps,
    write_evidence_status,
)
from wikiwar.historical import (
    EVENT_ENTITY,
    EVENT_TIMESTAMP,
    EVENT_TYPE,
    EVENT_USER_TEXT,
    PAGE_ID,
    PAGE_NAMESPACE_HISTORICAL,
    PAGE_TITLE,
    REVISION_ID,
    WIKI_DB,
)
from wikiwar.repository import (
    apply_cached_historical_evidence,
    load_historical_evidence,
    save_historical_evidence,
)
from wikiwar.schema import metadata


def test_revision_shards_for_page_ids_dedupes_matching_ranges() -> None:
    shards = [
        RevisionDumpShard("a.bz2", "https://example.test/a.bz2", 1, 100),
        RevisionDumpShard("b.bz2", "https://example.test/b.bz2", 101, 200),
        RevisionDumpShard("c.bz2", "https://example.test/c.bz2", 201, 300),
    ]

    selected = revision_shards_for_page_ids(shards, [15, 20, 175, 999])

    assert [shard.filename for shard in selected] == ["a.bz2", "b.bz2"]


def test_talk_page_ids_from_local_history_dumps(tmp_path: Path) -> None:
    path = tmp_path / "2026-05.enwiki.2017-01.tsv.bz2"
    row = history_tsv_row(page_id=43, page_title="Example", namespace=1, rev_id=101)
    import bz2

    with bz2.open(path, "wt", encoding="utf-8") as file:
        file.write("\t".join(row) + "\n")

    result = talk_page_ids_from_history_dumps(
        [
            CandidateEvidenceInput(
                wiki="enwiki",
                page_id=42,
                page_title="Example",
                period="history:2026-05:2017-01",
            )
        ],
        period="history:2026-05:2017-01",
        wiki="enwiki",
        history_dump_dir=tmp_path,
    )

    assert result == {42: 43}


def test_evidence_status_round_trips(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"

    write_evidence_status(status_file, {"status": "running", "phase": "downloading"})

    status = read_evidence_status(status_file)
    assert status["status"] == "running"
    assert status["phase"] == "downloading"
    assert status["updated_at"].endswith("Z")


def test_collects_article_and_talk_revisions_from_local_xml_dump(tmp_path: Path) -> None:
    dump_path = sample_revision_dump(tmp_path)
    candidates = [
        CandidateEvidenceInput(
            wiki="enwiki",
            page_id=42,
            page_title="Example",
            period="history:2026-05:2017-01",
        )
    ]

    collected = collect_revisions_from_xml_dumps(
        [dump_path],
        candidates,
        "history:2026-05:2017-01",
    )

    assert len(collected.articles[42]) == 7
    assert len(collected.talks[42]) == 1
    assert collected.articles[42][0]["content"] == "The article called him a leader of the party."
    assert collected.talks[42][0]["content"].startswith("Editors should discuss")


def test_local_evidence_payload_uses_dump_content_and_talk_debate(tmp_path: Path) -> None:
    dump_path = sample_revision_dump(tmp_path)
    candidates = [
        CandidateEvidenceInput(
            wiki="enwiki",
            page_id=42,
            page_title="Example",
            period="history:2026-05:2017-01",
            peak_score=120,
            revert_count=6,
            mutual_revert_pairs=1,
        )
    ]
    collected = collect_revisions_from_xml_dumps(
        [dump_path],
        candidates,
        "history:2026-05:2017-01",
    )

    payload = build_local_evidence_payload(
        article_revisions=collected.articles[42],
        talk_revisions=collected.talks[42],
        wiki="enwiki",
        page_id=42,
        page_title="Example",
        period="history:2026-05:2017-01",
        peak_score=120,
        revert_count=6,
        mutual_revert_pairs=1,
    )

    assert payload["source"] == "local_revision_dump"
    assert payload["revision_count"] == 7
    assert payload["segments"][0]["change_type"] == "swap"
    assert payload["segments"][0]["swap_values"] == ["leader", "dictator"]
    assert payload["segments"][0]["reverts"] >= 3
    assert payload["controversy"]["talk_evidence_count"] == 1
    assert "leader" in payload["controversy"]["talk_evidence"][0]["matched_terms"]


def test_historical_evidence_cache_round_trips_and_overlays_scoreboard_rows() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    payload = {
        "source": "local_revision_dump",
        "revision_count": 7,
        "segments": [{"segment": "dictator"}],
        "controversy": {
            "score": 207.47,
            "battle_score": 77.97,
            "talk_score": 42.0,
            "battle_count": 1,
            "talk_evidence_count": 1,
        },
    }

    with Session() as session:
        save_historical_evidence(
            session,
            period="history:2026-05:2017-01",
            wiki="enwiki",
            page_id=42,
            page_title="Example",
            source="local_revision_dump",
            payload=payload,
        )
        cached = load_historical_evidence(
            session,
            period="history:2026-05:2017-01",
            wiki="enwiki",
            page_id=42,
        )
        rows = apply_cached_historical_evidence(
            session,
            [
                {
                    "period": "history:2026-05:2017-01",
                    "wiki": "enwiki",
                    "page_id": 42,
                    "page_title": "Example",
                    "peak_score": 99,
                }
            ],
        )

    assert cached is not None
    assert cached["payload"]["source"] == "local_revision_dump"
    assert rows[0]["controversy_score"] == 207.47
    assert rows[0]["battle_score"] == 77.97
    assert rows[0]["talk_score"] == 42.0
    assert rows[0]["battle_count"] == 1
    assert rows[0]["talk_evidence_count"] == 1
    assert rows[0]["local_evidence_source"] == "local_revision_dump"


def test_historical_segments_do_not_call_api_without_explicit_fallback(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    def fail_fetch_revision_segments(**_: object) -> dict[str, object]:
        raise AssertionError("historical evidence endpoint should not call the API-backed segment fetcher")

    monkeypatch.setattr(wikiwar_app, "SessionLocal", Session)
    monkeypatch.setattr(wikiwar_app, "fetch_revision_segments", fail_fetch_revision_segments)

    payload = wikiwar_app.scoreboard_segments(
        wiki="enwiki",
        page_id=42,
        page_title="Example",
        period="history:2026-05:2017-01",
        historical=True,
    )

    assert payload["source"] == "local_evidence_missing"
    assert payload["segments"] == []


def sample_revision_dump(tmp_path: Path) -> Path:
    path = tmp_path / "sample-pages-meta-history.xml"
    path.write_text(
        """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/">
  <page>
    <title>Example</title>
    <ns>0</ns>
    <id>42</id>
    <revision>
      <id>1</id>
      <timestamp>2017-01-01T00:01:00Z</timestamp>
      <contributor><username>Alice</username><id>10</id></contributor>
      <comment>initial</comment>
      <text xml:space="preserve">The article called him a leader of the party.</text>
    </revision>
    <revision>
      <id>2</id>
      <timestamp>2017-01-01T00:02:00Z</timestamp>
      <contributor><username>Bob</username><id>20</id></contributor>
      <comment>restore dictator wording</comment>
      <text xml:space="preserve">The article called him a dictator of the party.</text>
    </revision>
    <revision>
      <id>3</id>
      <timestamp>2017-01-01T00:03:00Z</timestamp>
      <contributor><username>Alice</username><id>10</id></contributor>
      <comment>revert to leader wording</comment>
      <text xml:space="preserve">The article called him a leader of the party.</text>
    </revision>
    <revision>
      <id>4</id>
      <timestamp>2017-01-01T00:04:00Z</timestamp>
      <contributor><username>Bob</username><id>20</id></contributor>
      <comment>restore dictator wording</comment>
      <text xml:space="preserve">The article called him a dictator of the party.</text>
    </revision>
    <revision>
      <id>5</id>
      <timestamp>2017-01-01T00:05:00Z</timestamp>
      <contributor><username>Alice</username><id>10</id></contributor>
      <comment>rollback to leader wording</comment>
      <text xml:space="preserve">The article called him a leader of the party.</text>
    </revision>
    <revision>
      <id>6</id>
      <timestamp>2017-01-01T00:06:00Z</timestamp>
      <contributor><username>Bob</username><id>20</id></contributor>
      <comment>restore dictator wording</comment>
      <text xml:space="preserve">The article called him a dictator of the party.</text>
    </revision>
    <revision>
      <id>7</id>
      <timestamp>2017-01-01T00:07:00Z</timestamp>
      <contributor><username>Alice</username><id>10</id></contributor>
      <comment>revert to leader wording</comment>
      <text xml:space="preserve">The article called him a leader of the party.</text>
    </revision>
  </page>
  <page>
    <title>Talk:Example</title>
    <ns>1</ns>
    <id>43</id>
    <revision>
      <id>101</id>
      <timestamp>2017-01-01T00:08:00Z</timestamp>
      <contributor><username>Carol</username><id>30</id></contributor>
      <comment>wording dispute</comment>
      <text xml:space="preserve">Editors should discuss the disputed wording and reach consensus about whether leader or dictator is neutral.</text>
    </revision>
  </page>
</mediawiki>
""",
        encoding="utf-8",
    )
    return path


def history_tsv_row(*, page_id: int, page_title: str, namespace: int, rev_id: int) -> list[str]:
    row = [""] * 78
    row[WIKI_DB] = "enwiki"
    row[EVENT_ENTITY] = "revision"
    row[EVENT_TYPE] = "create"
    row[EVENT_TIMESTAMP] = "2017-01-01 00:00:00.0"
    row[EVENT_USER_TEXT] = "TalkEditor"
    row[PAGE_ID] = str(page_id)
    row[PAGE_TITLE] = page_title
    row[PAGE_NAMESPACE_HISTORICAL] = str(namespace)
    row[REVISION_ID] = str(rev_id)
    return row
