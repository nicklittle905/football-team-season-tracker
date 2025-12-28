# Football Team Season Tracker

End-to-end pipeline for football-data.org → DuckDB → dbt → Streamlit. Ingests full competition data (teams/matches), builds standings/marts, and renders a FotMob-inspired dashboard with team selection, logos, and per-team views.

## Features
- Ingest teams/matches (with crest URLs) for a competition/season into DuckDB.
- dbt transforms: standings per matchday, latest league table, per-team match facts.
- Streamlit dashboard:
  - Sidebar: competition/season, league table picker (click any row to select a team), refresh pipeline (enabled after selection), debug toggle.
  - Overview: KPIs, form tiles (all matches), position-through-time chart with promotion/playoff/relegation bands.
  - Matches: searchable match list + match detail card.
  - Table: full league table with form column and selected team highlight.
  - About: data source/limitations.

## Prerequisites
- Python 3.9+
- football-data.org API token
- DuckDB (CLI optional)
- dbt-duckdb, Streamlit, Pandas, Altair

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# App + dbt deps (if not already installed globally)
pip install streamlit pandas altair dbt-duckdb
```

## Configure environment
Copy `.env.example` to `.env` and set:
```
FOOTBALL_DATA_TOKEN=your_token
COMP_CODE=ELC
SEASON=2025
DUCKDB_PATH=warehouse/season_tracker.duckdb  # set to the DB you want all components to share
```

## Ingest raw data
From repo root:
```bash
python -m ingest.load_raw      # or: python ingest/load_raw.py
# optional: --full-refresh to truncate raw tables before load
```
This creates/updates `warehouse/charlton.duckdb` with `raw_teams`, `raw_matches`, and `ingest_runs`.

## Run dbt transforms
From `charlton_dbt/`:
```bash
dbt run
dbt test    # optional
```
Key models:
- `fct_team_match`: one row per team per match (finished matches only)
- `fct_standings_matchday`: cumulative standings per matchday with ranking
- `mart_league_table_current`: latest matchday league table (with crest)
- `mart_team_position_through_time`: standings history for all teams
- `mart_team_last_5`: per-team match list (all matches, newest first)

## Run the pipeline end-to-end
From repo root:
```bash
# Build marts for a specific team context (e.g., 348):
python -m pipeline.run_pipeline --team-id 348
```
This runs ingest + dbt (passing team_id to any team-filtered marts).

## Launch the dashboard
From repo root (after ingest + dbt):
```bash
streamlit run app.py
```
The dashboard loads after you select a team by clicking any row in the sidebar league table; the Refresh button stays disabled until a team is selected. Toggle “Debug” to print query diagnostics.

## Troubleshooting
- **Missing/invalid DB file**: delete/move the DuckDB file you configured (e.g., `warehouse/season_tracker.duckdb`) and re-run ingest.
- **Empty charts**: toggle “Debug” to inspect the data feeding the charts; ensure ingest + dbt ran after selecting your team (via the sidebar refresh).
- **API issues/rate limits**: ingest surfaces HTTP errors; verify your football-data.org plan/limits.
