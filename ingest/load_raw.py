import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import duckdb
import requests
from dotenv import load_dotenv

# Allow running as `python ingest/load_raw.py` by adding project root to sys.path
if __package__ is None and __name__ == "__main__":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from ingest.config import load_settings

API_BASE = "https://api.football-data.org/v4"


def api_get(path: str, token: str, params: Optional[dict] = None) -> dict:
    headers = {"X-Auth-Token": token}
    resp = requests.get(f"{API_BASE}{path}", headers=headers, params=params, timeout=30)
    # Helpful error details if your plan doesn't allow a comp, rate limited, etc.
    if resp.status_code >= 400:
        raise RuntimeError(f"API error {resp.status_code}: {resp.text}")
    return resp.json()


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS ingest_runs (
        run_id VARCHAR PRIMARY KEY,
        run_ts_utc TIMESTAMP,
        comp_code VARCHAR,
        season INTEGER,
        status VARCHAR,
        details VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_teams (
        team_id BIGINT PRIMARY KEY,
        name VARCHAR,
        short_name VARCHAR,
        tla VARCHAR,
        crest VARCHAR,
        fetched_at_utc TIMESTAMP
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS raw_matches (
        match_id BIGINT PRIMARY KEY,
        competition_code VARCHAR,
        season_start_year INTEGER,
        utc_date TIMESTAMP,
        status VARCHAR,
        matchday INTEGER,
        stage VARCHAR,
        group_name VARCHAR,

        home_team_id BIGINT,
        home_team_name VARCHAR,
        away_team_id BIGINT,
        away_team_name VARCHAR,

        home_score_full INTEGER,
        away_score_full INTEGER,
        home_score_half INTEGER,
        away_score_half INTEGER,

        winner VARCHAR,
        last_updated_utc TIMESTAMP,
        fetched_at_utc TIMESTAMP,

        raw_json VARCHAR
    );
    """)


def upsert_teams(con: duckdb.DuckDBPyConnection, teams_payload: dict) -> int:
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
    teams = teams_payload.get("teams", [])

    rows = []
    for t in teams:
        rows.append((
            int(t["id"]),
            t.get("name"),
            t.get("shortName"),
            t.get("tla"),
            t.get("crest"),
            fetched_at,
        ))

    if not rows:
        return 0

    con.executemany("""
        INSERT INTO raw_teams (team_id, name, short_name, tla, crest, fetched_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(team_id) DO UPDATE SET
            name=excluded.name,
            short_name=excluded.short_name,
            tla=excluded.tla,
            crest=excluded.crest,
            fetched_at_utc=excluded.fetched_at_utc;
    """, rows)

    return len(rows)


def upsert_matches(con: duckdb.DuckDBPyConnection, comp_code: str, season: int, matches_payload: dict) -> int:
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
    matches = matches_payload.get("matches", [])

    rows = []
    for m in matches:
        score = m.get("score", {}) or {}
        ft = score.get("fullTime", {}) or {}
        ht = score.get("halfTime", {}) or {}

        # utcDate comes as ISO string; DuckDB can parse via TIMESTAMP cast if we pass as string,
        # but we’ll parse to a naive datetime for consistency.
        utc_date_str = m.get("utcDate")
        utc_dt = None
        if utc_date_str:
            utc_dt = datetime.fromisoformat(utc_date_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

        last_updated = None
        lu_str = m.get("lastUpdated")
        if lu_str:
            last_updated = datetime.fromisoformat(lu_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

        home = m.get("homeTeam", {}) or {}
        away = m.get("awayTeam", {}) or {}

        rows.append((
            int(m["id"]),
            comp_code,
            int(season),
            utc_dt,
            m.get("status"),
            m.get("matchday"),
            m.get("stage"),
            m.get("group"),

            int(home["id"]) if home.get("id") is not None else None,
            home.get("name"),
            int(away["id"]) if away.get("id") is not None else None,
            away.get("name"),

            ft.get("home"),
            ft.get("away"),
            ht.get("home"),
            ht.get("away"),

            score.get("winner"),
            last_updated,
            fetched_at,

            json.dumps(m, ensure_ascii=False)
        ))

    if not rows:
        return 0

    con.executemany("""
        INSERT INTO raw_matches (
            match_id, competition_code, season_start_year, utc_date, status, matchday, stage, group_name,
            home_team_id, home_team_name, away_team_id, away_team_name,
            home_score_full, away_score_full, home_score_half, away_score_half,
            winner, last_updated_utc, fetched_at_utc, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id) DO UPDATE SET
            competition_code=excluded.competition_code,
            season_start_year=excluded.season_start_year,
            utc_date=excluded.utc_date,
            status=excluded.status,
            matchday=excluded.matchday,
            stage=excluded.stage,
            group_name=excluded.group_name,
            home_team_id=excluded.home_team_id,
            home_team_name=excluded.home_team_name,
            away_team_id=excluded.away_team_id,
            away_team_name=excluded.away_team_name,
            home_score_full=excluded.home_score_full,
            away_score_full=excluded.away_score_full,
            home_score_half=excluded.home_score_half,
            away_score_half=excluded.away_score_half,
            winner=excluded.winner,
            last_updated_utc=excluded.last_updated_utc,
            fetched_at_utc=excluded.fetched_at_utc,
            raw_json=excluded.raw_json;
    """, rows)

    return len(rows)


def main():
    load_dotenv()  # reads .env if present
    settings = load_settings()

    parser = argparse.ArgumentParser(description="Load football-data.org raw data into DuckDB")
    parser.add_argument("--full-refresh", action="store_true", help="Truncate raw tables before loading")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(settings.duckdb_path), exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    con = duckdb.connect(settings.duckdb_path)
    try:
        ensure_schema(con)

        if args.full_refresh:
            con.execute("DELETE FROM raw_matches;")
            con.execute("DELETE FROM raw_teams;")

        con.execute("""
            INSERT INTO ingest_runs (run_id, run_ts_utc, comp_code, season, status, details)
            VALUES (?, ?, ?, ?, 'STARTED', NULL)
        """, (run_id, run_ts, settings.comp_code, settings.season))

        # 1) Teams in competition (season-param supported)
        teams_payload = api_get(f"/competitions/{settings.comp_code}/teams", settings.token, params={"season": settings.season})
        n_teams = upsert_teams(con, teams_payload)

        # 2) Matches in competition (season-param supported)
        matches_payload = api_get(f"/competitions/{settings.comp_code}/matches", settings.token, params={"season": settings.season})
        n_matches = upsert_matches(con, settings.comp_code, settings.season, matches_payload)

        con.execute("""
            UPDATE ingest_runs
            SET status='SUCCESS',
                details=?
            WHERE run_id=?
        """, (f"teams={n_teams}, matches={n_matches}", run_id))

        print(f"[OK] Loaded teams={n_teams}, matches={n_matches} into {settings.duckdb_path}")

    except Exception as e:
        con.execute("""
            UPDATE ingest_runs
            SET status='FAILED',
                details=?
            WHERE run_id=?
        """, (str(e)[:5000], run_id))
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
