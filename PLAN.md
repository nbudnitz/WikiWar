# WikiWar Implementation Plan

## Product Goal

WikiWar should surface pages where Wikipedia editing behavior is currently contentious, not merely busy. The core product should answer three questions:

1. Which pages are in an edit war right now?
2. Why did the system classify them that way?
3. Which pages are most warred over a recent or historical period?

The first version should focus on English Wikipedia article pages, then expand to other language Wikipedias once the classifier and data model are stable.

## Working Definition

Wikipedia describes edit warring as editors repeatedly overriding each other's contributions; the three-revert rule is a useful signal but not the full definition. WikiWar should therefore treat a war as a high-conflict editing episode with:

- Fast editing velocity relative to normal page activity.
- Multiple human editors.
- Reverts or undo-like edits.
- Mutuality, meaning editors revert each other rather than one patroller reverting many vandals.
- Persistence across a rolling time window.

The classifier should avoid equating vandalism cleanup, bot work, page maintenance, and ordinary high-volume editing with edit wars.

## Data Sources

Use a strict source boundary:

- Live and near-real-time features come from EventStreams plus limited API enrichment.
- Historical analysis comes from downloadable Wikimedia datasets.
- Multi-year historical scoring must not crawl page histories through the MediaWiki Action API.

### Live Stream

Use Wikimedia EventStreams `recentchange` as the source of truth for current edit events:

- Endpoint: `https://stream.wikimedia.org/v2/stream/recentchange`
- Protocol: Server-Sent Events.
- First filters: `server_name = "en.wikipedia.org"`, namespace `0`, non-bot edits.
- Persist every event before enrichment so the pipeline is replayable.

### API Enrichment

Use the MediaWiki Action API only for bounded live and user-facing enrichment:

- Enriching active candidate pages.
- Short catch-up windows after downtime via `list=recentchanges`.
- Fetching revision metadata, comments, SHA/content identity when needed, parent revision, and nearby page history for selected live events via `prop=revisions`.
- Page metadata for redirects, protection state, and talk-page activity.
- User-facing drill-downs and small spot checks.

Do not use the Action API for broad historical backfill, per-page multi-year crawling, or historical scoreboard generation.

### ML Enrichment

Use Wikimedia Lift Wing revert-risk models cautiously as supporting features:

- Revert risk is useful for distinguishing suspicious/vandalism-like edits from content disputes.
- It should not define an edit war by itself.
- Avoid using the model for unsupported languages, bot revisions, first revisions, or pages where the model card says it is inappropriate.

### Historical Backfill

Use downloadable Wikimedia datasets for historical scoreboards:

- `mediawiki_history` dumps contain revision, user, and page events back to 2001.
- They include useful precomputed revert fields.
- For large wikis such as `enwiki`, process monthly dump partitions.
- Generate scoreboard snapshots in offline batch jobs.
- Use dumps for historical rankings and model-training data, not for real-time tracking.

Incremental dumps can help with delayed batch reconciliation, but they are not up-to-the-minute and omit some event types. They should not replace the full `mediawiki_history` snapshot for historical scoreboard generation.

## System Architecture

### Recommended Stack

- Backend/API: Python + FastAPI.
- Stream worker: Python async worker consuming EventStreams.
- Queue: Redis Streams or NATS for local durability between ingest and scoring.
- Database: PostgreSQL, with TimescaleDB if rolling-window queries become heavy.
- Frontend: Next.js or Vite React with TypeScript.
- Visualization: D3 or Visx for timelines and revert graphs.
- Packaging: Docker Compose for local development.

Python keeps the classifier and future model training straightforward. TypeScript keeps the UI maintainable.

### Services

1. `ingest-worker`
   - Connects to EventStreams.
   - Filters relevant events.
   - Writes append-only raw events.
   - Emits normalized edit events to the queue.

2. `enrichment-worker`
   - Fetches revision/page details for candidate edits.
   - Detects likely reverts from tags, comments, revision identity, and undo/rollback metadata where available.
   - Writes normalized edit and revert records.

3. `scoring-worker`
   - Maintains rolling page windows.
   - Builds per-page revert graphs.
   - Computes conflict score and severity.
   - Opens, updates, and closes war episodes.

4. `api-server`
   - Serves current wars, page detail, historical episodes, and scoreboards.
   - Streams live updates to the frontend over WebSocket or SSE.

5. `frontend`
   - Displays live tracker, page detail, and scoreboard.

## Data Model

### Core Tables

`raw_events`
- `id`
- `stream_id`
- `received_at`
- `payload_json`

`edits`
- `wiki`
- `page_id`
- `page_title`
- `namespace`
- `rev_id`
- `parent_rev_id`
- `timestamp`
- `user_id`
- `user_text`
- `user_is_bot`
- `user_is_anonymous`
- `comment`
- `tags`
- `minor`
- `old_len`
- `new_len`
- `source`

`reverts`
- `wiki`
- `page_id`
- `rev_id`
- `reverter_user`
- `reverted_user`
- `reverted_rev_id`
- `detector`
- `confidence`
- `timestamp`

`page_windows`
- `wiki`
- `page_id`
- `window_start`
- `window_size`
- `edit_count`
- `human_edit_count`
- `unique_human_editors`
- `revert_count`
- `mutual_revert_count`
- `mutual_revert_pairs`
- `top_reverter_share`
- `revert_density`
- `edit_velocity_z`
- `conflict_score`
- `severity`

`war_episodes`
- `wiki`
- `page_id`
- `started_at`
- `ended_at`
- `peak_score`
- `score_area`
- `total_edits`
- `total_reverts`
- `total_mutual_reverts`
- `participants`
- `status`

`scoreboard_snapshots`
- `period`
- `rank`
- `wiki`
- `page_id`
- `page_title`
- `score_area`
- `peak_score`
- `war_minutes`
- `episode_count`

## Revert Detection

Use layered detection and store confidence:

1. Explicit revert signals
   - Rollback/undo/manual-revert tags.
   - Edit summaries containing common revert patterns.

2. Revision identity
   - A revision restores content to a prior state on the same page.
   - Attribute the reverted editor from the revision range that was undone.

3. Mutual revert graph
   - Add a directed edge from reverter to reverted editor.
   - A mutual pair exists when both directions occur within a rolling window.
   - Weight pairs by the smaller side of the exchange so one-sided vandalism cleanup does not dominate.

4. Exclusions
   - Self-reverts.
   - Bot edits.
   - Obvious vandalism-only bursts where many low-trust edits are reverted by one or two patrollers.
   - Page moves, log-only events, and non-article namespaces in the initial release.

## Classifier

### Version 0: Interpretable Rules

Start with a transparent rule-based classifier. Example thresholds for a 24-hour window:

- `human_edit_count >= 8`
- `unique_human_editors >= 3`
- `revert_count >= 4`
- `mutual_revert_pairs >= 1`
- `revert_density >= 0.25`
- `top_reverter_share < 0.75`

Severity bands:

- `watch`: score 40-59
- `skirmish`: score 60-74
- `war`: score 75-89
- `major`: score 90-100

Use shorter 5-minute and 1-hour windows for the live tracker, but require enough 24-hour evidence before labeling something as a full war.

### Conflict Score

Compute a 0-100 score from interpretable components:

- Edit velocity relative to page baseline.
- Revert density.
- Mutual revert strength.
- Number of human participants.
- Recency-weighted burstiness.
- Talk-page or protection activity near the same time.
- Penalty for bot/maintenance/vandalism-cleanup signatures.

Each score returned by the API should include feature contributions so the UI can explain the classification.

### Version 1: Trained Model

After collecting data, train a supervised or weakly supervised model:

- Positive labels: administrator edit-warring reports, page-protection reasons mentioning edit warring, dispute templates, known historical edit-war pages, and high-confidence manually reviewed episodes.
- Negative labels: high-traffic pages without mutual reverts, vandalism cleanup bursts, bot-heavy maintenance pages, and random article windows.
- Model: logistic regression or gradient-boosted trees before using anything opaque.
- Evaluation: precision at top K, false-positive review by category, calibration by wiki/language, and manual review of the daily top 50.

The trained model should augment the rule engine, not replace explainability.

## Live War Tracker

The live tracker should show:

- Current ranked list of active wars.
- Current severity, score, and score trend.
- Edits/reverts in the last 5 minutes, 1 hour, and 24 hours.
- Active participants count.
- Last edit time.
- Direct links to the Wikipedia page, history, diff, and talk page.

Page detail should include:

- Score-over-time timeline.
- Edit/revert timeline.
- Revert graph between editors.
- Recent edit summaries.
- Major diffs that caused score jumps.
- Episode history for the page.
- Explanation panel showing feature contributions.

## Scoreboard

The scoreboard should rank pages by accumulated conflict, not just raw edit count.

Recommended metrics:

- `war_minutes`: minutes spent above the war threshold.
- `score_area`: sum of conflict score above threshold over time.
- `peak_score`: maximum score in the period.
- `episode_count`: number of separate war episodes.
- `mutual_revert_count`: total mutual reverts.

Views:

- Live: last 1 hour and 24 hours.
- Recent: last 7 days and 30 days.
- Historical: monthly and all-time once dump processing exists.
- Filters by wiki, topic, namespace, and active/resolved status.

## Frontend Shape

First screen should be the working dashboard, not a landing page:

- Left rail or top tabs: Live, Scoreboard, Page Detail, Methodology.
- Main live table with dense, sortable rows.
- Side panel for selected page details.
- Clear visual distinction between high edit volume and high conflict.
- Small multiples/timelines over decorative graphics.

Avoid framing the project as an accusation against editors. Use language like "conflict signal", "revert activity", and "classified episode" rather than implying bad faith.

## Implementation Milestones

### Milestone 1: Live Ingest Skeleton

- Create FastAPI backend.
- Create stream worker for EventStreams.
- Store raw and normalized edits in PostgreSQL.
- Add reconnect, backoff, checkpointing, and basic health checks.

Success: local service stores live English Wikipedia article edits for several hours without data loss.

### Milestone 2: First Classifier

- Implement explicit revert detection from tags/comments.
- Compute rolling 5-minute, 1-hour, and 24-hour windows.
- Add rule-based conflict score.
- Store active `war_episodes`.

Success: API returns plausible active candidate pages with explainable scores.

### Milestone 3: Dashboard MVP

- Build live tracker UI.
- Add sortable scoreboard table.
- Add page detail timeline and outbound Wikipedia links.
- Stream updates without full-page refresh.

Success: user can open the app and watch current conflict candidates update live.

### Milestone 4: Stronger Revert Detection

- Fetch nearby revision history for candidate pages.
- Detect identity reverts by revision/content state.
- Build mutual revert graph.
- Add vandalism-cleanup and bot/maintenance exclusions.

Success: obvious vandalism cleanup drops in rank; real mutual-revert disputes rise.

### Milestone 5: Historical Scoreboard

- Download and process partitioned `mediawiki_history` dumps for `enwiki`.
- Generate monthly and all-time scoreboard snapshots.
- Reconcile historical scoring with live scoring.

Success: scoreboard can compare live events against historical war episodes.

### Milestone 6: Model Training

- Build labeled dataset.
- Add manual review workflow for top candidates.
- Train interpretable baseline model.
- Calibrate thresholds and severity bands.

Success: classifier precision improves on reviewed top candidates without losing explainability.

### Milestone 7: Multi-Wiki Expansion

- Add wiki/language configuration.
- Respect Lift Wing model language support.
- Normalize cross-wiki page titles and namespaces.
- Add per-wiki thresholds or calibration.

Success: tracker supports multiple major Wikipedias with sane false-positive rates.

## Operational Concerns

- Follow Wikimedia API etiquette: descriptive User-Agent, rate limiting, request batching, and caching.
- Use APIs only on live/enrichment paths, with retry backoff and respect for `Retry-After` responses.
- Historical jobs should stream and decompress dump files, process partitions offline, and write compact aggregate outputs.
- Do not store every historical raw row in the application database unless a specific research workflow requires it; store reusable aggregates, candidate episodes, and scoreboard snapshots instead.
- Keep raw event storage append-only for replay/debugging.
- Treat anonymous editor identifiers as public but sensitive; do not add unnecessary profiling.
- Do not expose harassment-oriented features such as "worst editor" leaderboards.
- Include classifier caveats in the product methodology.
- Monitor stream lag, API error rates, queue depth, scoring latency, and false-positive review outcomes.

## References

- Wikimedia EventStreams: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
- Wikimedia API rate limits: https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits
- MediaWiki API etiquette: https://www.mediawiki.org/wiki/API:Etiquette
- MediaWiki RecentChanges API: https://www.mediawiki.org/wiki/API:RecentChanges
- MediaWiki Revisions API: https://www.mediawiki.org/wiki/API:Revisions
- Wikimedia MediaWiki History dumps: https://dumps.wikimedia.org/other/mediawiki_history/readme.html
- Wikimedia data dumps overview: https://meta.wikimedia.org/wiki/Data_dumps
- Lift Wing multilingual revert risk: https://meta.wikimedia.org/wiki/Machine_learning_models/Production/Multilingual_revert_risk
- Lift Wing API reference: https://api.wikimedia.org/wiki/Lift_Wing_API/Reference/Get_reverted_risk_multilingual_prediction
- Wikipedia edit-warring policy: https://en.wikipedia.org/wiki/Wikipedia:Edit_warring
- Yasseri et al., "Dynamics of Conflicts in Wikipedia": https://doi.org/10.1371/journal.pone.0038869
