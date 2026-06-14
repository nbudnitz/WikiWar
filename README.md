# WikiWar

WikiWar tracks live Wikipedia edit-conflict signals. This MVP implements Milestones 1-3 from `PLAN.md`:

- EventStreams ingestion for English Wikipedia article edits.
- Raw event, normalized edit, revert, rolling-window, and active episode storage.
- Explicit revert detection from tags and edit summaries.
- Explainable 5-minute, 1-hour, and 24-hour conflict scoring.
- A live dashboard, scoreboard, page detail timeline, and Wikipedia links.

Historical scoreboards are generated from downloadable `mediawiki_history` dumps, not Action API crawling.
Historical battle/talk evidence is also local-first: build it from downloaded revision-content XML dumps and cache compact results in the app database. The MediaWiki API is reserved for live views, explicit spot checks, and opt-in fallback.

## Run Locally

Install dependencies:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
```

Start with the default local SQLite database:

```sh
uvicorn wikiwar.app:app --reload
```

Open `http://127.0.0.1:8000`.

## Run With PostgreSQL

```sh
docker compose up --build
```

Open `http://127.0.0.1:8000`.

## Configuration

- `WIKIWAR_DATABASE_URL`: SQLAlchemy database URL. Defaults to `sqlite:///./data/wikiwar.db`.
- `WIKIWAR_START_INGEST`: set to `false` to run the API without the EventStreams worker.
- `WIKIWAR_USER_AGENT`: descriptive Wikimedia User-Agent with contact information.
- `WIKIWAR_SERVER_NAME`: defaults to `en.wikipedia.org`.
- `WIKIWAR_DB`: defaults to `enwiki`.
- `WIKIWAR_NAMESPACE`: defaults to `0` for article pages.

## Sanity Checks

```sh
pytest
python -m compileall wikiwar
```

The dashboard may initially show no rows until relevant non-bot article edits arrive from EventStreams.

## Historical Data

Start with one small partition before attempting modern enwiki months. The historical pipeline works from local `.tsv.bz2` files and writes compact `scoreboard_snapshots`.

List available partitions for a snapshot:

```sh
python -m wikiwar.historical list --snapshot 2026-05 --wiki enwiki
```

Download one partition over HTTPS:

```sh
python -m wikiwar.historical download --snapshot 2026-05 --wiki enwiki --partition 2001-01
```

For full historical runs, bulk-prefetch the dump files first with `rsync` instead of downloading one partition at a time during backfill:

```sh
mkdir -p data/dumps data/logs
python -m wikiwar.historical prefetch \
  --snapshot 2026-05 \
  --wiki enwiki \
  --output-dir data/dumps
```

To resume from a specific month:

```sh
python -m wikiwar.historical prefetch \
  --snapshot 2026-05 \
  --wiki enwiki \
  --output-dir data/dumps \
  --start-partition 2014-10
```

The prefetch command uses Wikimedia dump mirrors over `rsync`, keeps partial files, skips files that already match by size, and can be rerun safely.

Process it into a historical scoreboard period:

```sh
python -m wikiwar.historical process data/dumps/2026-05.enwiki.2001-01.tsv.bz2 --period history:2026-05:2001-01
```

The dashboard Scoreboard tab groups processed historical months into year scoreboards after refresh. Month partitions remain the backfill write unit, so a newly completed `history:2026-05:2014-09` partition is picked up automatically by the `history-year:2026-05:2014` scoreboard without rewriting earlier months. For larger runs, process partitions in chronological order so revert edges can be reconstructed when a reverting revision appears in a later file.

After prefetching, run the resumable local backfill:

```sh
mkdir -p data/logs
nohup python -m wikiwar.historical backfill \
  --snapshot 2026-05 \
  --wiki enwiki \
  --output-dir data/dumps \
  --limit 100 \
  --min-score 40 \
  --sleep-seconds 0 \
  --keep-downloads \
  --workers 3 \
  > data/logs/historical-backfill.log 2>&1 &
```

The backfill processes local monthly partitions and skips periods that already exist in `scoreboard_snapshots`, so the same command can be rerun after interruption. If a required local file is missing, the backfill can still fetch it over HTTPS, but the preferred bulk path is `prefetch` first, parse locally second.

Build local battle/talk evidence from downloaded `pages-meta-history` XML dumps after the compact scoreboard rows exist:

```sh
python -m wikiwar.evidence backfill \
  --period history-year:2026-05:2017 \
  --dump data/dumps/enwiki-20260501-pages-meta-history1.xml-p1p41242.bz2 \
  --dump data/dumps/enwiki-20260501-pages-meta-history2.xml-p41243p151573.bz2 \
  --limit 100
```

Pass every XML shard needed to cover the candidate article pages and their talk pages. The evidence backfill streams/decompresses the dump files, extracts only the selected period, writes compact `historical_evidence_cache` rows, and does not call the MediaWiki API. Historical scoreboard drill-downs use this cache by default; if no local evidence exists yet, the UI reports that the page/period has not been backfilled. Use `allow_api_fallback=true` on `/api/scoreboard/segments` only for deliberate spot checks.

Monitor long historical jobs:

```sh
tail -f data/logs/historical-prefetch.log
tail -f data/logs/historical-backfill.log
du -sh data/dumps
```
