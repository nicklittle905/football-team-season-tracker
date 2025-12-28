from datetime import datetime
import os
from pathlib import Path
import textwrap
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from pipeline.run_pipeline import run_refresh

DB_PATH = Path(__file__).resolve().parent / "warehouse/season_tracker.duckdb"
COMP_CODE = os.getenv("COMP_CODE", "ELC")
SEASON = os.getenv("SEASON", "2025")

st.set_page_config(page_title="Season Tracker", layout="wide")

# -----------------------------------------------------------------------------
# Session + caching helpers
# -----------------------------------------------------------------------------
session_defaults = {
    "selected_team_id": None,
    "selected_team_name": None,
    "selected_team_crest_url": None,
    "refreshing": False,
    "refresh_result": None,
    "last_refreshed_team_id": None,
    "last_refreshed_ts": None,
    "last_refresh_duration": None,
    "cache_buster": 0,
    "debug_mode": False,
}
for key, value in session_defaults.items():
    st.session_state.setdefault(key, value)


def cache_key() -> int:
    mtime = 0
    try:
        mtime = int(DB_PATH.stat().st_mtime)
    except OSError:
        mtime = 0
    return (int(st.session_state.get("cache_buster", 0)), mtime)


@st.cache_data(show_spinner=False)
def query_df(sql: str, params: Optional[List[Any]], cache_seed: Any) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    param_tuple = tuple(params) if params else tuple()
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            return con.execute(sql, param_tuple).df()
    except duckdb.Error:
        return pd.DataFrame()


def invalidate_cache() -> None:
    st.session_state["cache_buster"] = st.session_state.get("cache_buster", 0) + 1


def fetch_teams() -> List[Tuple[int, str]]:
    # Try staged first, then raw with proper aliases.
    staged = query_df(
        "select team_id, team_name, team_crest_url from stg_raw_teams order by team_name",
        None,
        cache_key(),
    )
    if not staged.empty:
        return [
            {"team_id": int(r.team_id), "team_name": str(r.team_name), "team_crest_url": r.team_crest_url}
            for _, r in staged.iterrows()
        ]

    raw = query_df(
        "select team_id, name as team_name, coalesce(crest_url, crest) as team_crest_url from raw_teams order by name",
        None,
        cache_key(),
    )
    if not raw.empty:
        return [
            {"team_id": int(r.team_id), "team_name": str(r.team_name), "team_crest_url": r.team_crest_url}
            for _, r in raw.iterrows()
        ]
    return []


def render_empty(message: str) -> None:
    st.info(message + " Click Refresh to build data.")


def table_form_badges(results: List[str]) -> str:
    colors = {"W": "#22c55e", "D": "#e2e8f0", "L": "#ef4444"}
    return "".join(
        f'<span style="display:inline-block;padding:2px 8px;margin-right:4px;border-radius:999px;background:{colors.get(r, "#e2e8f0")};color:#0f172a;font-weight:700;font-size:0.8rem;">{r}</span>'
        for r in results
    )

# Preload league table for sidebar selection (reused later for main content).
league_table = query_df("select * from mart_league_table_current order by position", None, cache_key())


# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Season Tracker")
    st.caption(f"Competition: **{COMP_CODE}** · Season: **{SEASON}**")
    st.markdown(
        """
        <style>
        div[data-testid="stSidebar"] button[data-testid="baseButton-secondary"] {
            justify-content: flex-start;
            text-align: left;
            font-family: monospace;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    teams = fetch_teams()
    teams_lookup = {t["team_id"]: t for t in teams}
    selected_team_id = st.session_state["selected_team_id"]
    selected_team_name = st.session_state["selected_team_name"]
    selected_team_crest_url = st.session_state.get("selected_team_crest_url")

    st.markdown("#### League table")
    if league_table.empty:
        st.warning("League table not available yet.")
        # Fallback to raw team list if present
        if teams:
            options = teams
            idx = None
            if selected_team_id is not None:
                try:
                    idx = next(i for i, t in enumerate(options) if t["team_id"] == selected_team_id)
                except StopIteration:
                    idx = None
            selected = st.selectbox(
                "Team",
                options=options,
                index=idx,
                format_func=lambda opt: opt["team_name"],
                placeholder="Select a team",
            )
            if selected:
                selected_team_id = selected["team_id"]
                selected_team_name = selected["team_name"]
                selected_team_crest_url = selected.get("team_crest_url")
    else:
        picker_df = league_table[["position", "team_name", "points", "team_id"]].copy()
        picker_df.rename(columns={"position": "Position", "team_name": "Team", "points": "Points", "team_id": "Team ID"}, inplace=True)
        st.markdown(
            "<div style='font-size:0.8rem;color:#94a3b8;font-family:monospace;text-align:center;'>"
            "POS | TEAM                     | PTS"
            "</div>",
            unsafe_allow_html=True,
        )
        for _, row in picker_df.iterrows():
            pos = int(row["Position"])
            team = textwrap.shorten(str(row["Team"]), width=24, placeholder="…")
            pts = int(row["Points"]) if pd.notna(row["Points"]) else 0
            tid = int(row["Team ID"])
            pos_str = str(pos).ljust(3)
            team_str = team.ljust(24)
            pts_str = str(pts).ljust(3)
            label = f"{pos_str}| {team_str}| {pts_str}"
            if st.button(label, key=f"team_row_{tid}", use_container_width=True, type="secondary"):
                selected_team_id = tid
                selected_team_name = team
                selected_team_crest_url = teams_lookup.get(selected_team_id, {}).get("team_crest_url")

    st.session_state["selected_team_id"] = selected_team_id
    st.session_state["selected_team_name"] = selected_team_name
    st.session_state["selected_team_crest_url"] = selected_team_crest_url

    refresh_disabled = st.session_state["refreshing"] or selected_team_id is None
    col_btn, col_dbg = st.columns([3, 1])
    with col_btn:
        if st.button("Refresh pipeline", type="primary", disabled=refresh_disabled):
            st.session_state["refreshing"] = True
            with st.spinner(f"Running ingest + dbt for team_id {selected_team_id}…"):
                start = datetime.utcnow()
                try:
                    result = run_refresh(int(selected_team_id))
                except Exception as exc:  # noqa: BLE001
                    result = {"ingest_ok": False, "dbt_ok": False, "ingest_stdout": str(exc), "dbt_stdout": ""}
                duration = (datetime.utcnow() - start).total_seconds()
                st.session_state["refresh_result"] = result
                st.session_state["refreshing"] = False
                if result.get("ingest_ok") and result.get("dbt_ok"):
                    st.session_state["last_refreshed_team_id"] = selected_team_id
                    st.session_state["last_refreshed_ts"] = datetime.utcnow()
                    st.session_state["last_refresh_duration"] = duration
                    invalidate_cache()
                    st.success("Pipeline completed.")
                    st.rerun()
                else:
                    st.error("Pipeline failed.")
                    with st.expander("See logs"):
                        st.write(result)
    with col_dbg:
        st.session_state["debug_mode"] = st.checkbox("Debug", value=st.session_state["debug_mode"])

    if st.session_state.get("last_refreshed_ts"):
        ts = st.session_state["last_refreshed_ts"].strftime("%Y-%m-%d %H:%M:%S UTC")
        dur = st.session_state.get("last_refresh_duration")
        st.caption(f"Last refresh: {ts} ({dur:.1f}s)")

# Early exit if no team selected yet
if selected_team_id is None:
    st.info("Select a team to view the dashboard.")
    st.stop()

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
header_col1, header_col2 = st.columns([5, 1])
with header_col1:
    crest_html = f'<img src="{selected_team_crest_url}" width="72" style="display:block;" />' if selected_team_crest_url else ""
    team_label = selected_team_name or "Select a team"
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:12px;">
          <div>{crest_html}</div>
          <div style="display:flex;flex-direction:column;gap:2px;">
            <div style="color:#94a3b8;font-size:0.9rem;">{COMP_CODE} · {SEASON}</div>
            <div style="font-size:2rem;font-weight:800;line-height:1.1;">{team_label}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with header_col2:
    st.caption(f"Team ID: {selected_team_id if selected_team_id is not None else '—'}")

# -----------------------------------------------------------------------------
# Data pulls
# -----------------------------------------------------------------------------
if selected_team_id is None:
    st.info("Select a team to view the dashboard.")
    st.stop()

pos_history = query_df(
    """
    select
      s.matchday,
      s.last_match_date as as_of_date,
      s.position,
      s.points,
      s.gd,
      tm.result,
      tm.goals_for,
      tm.goals_against,
      opp.team_name as opponent,
      opp.team_crest_url as opponent_crest_url
    from fct_standings_matchday s
    left join fct_team_match tm
      on tm.matchday = s.matchday
     and tm.team_id = s.team_id
    left join stg_raw_teams opp
      on tm.opponent_team_id = opp.team_id
    where s.team_id = ?
    order by s.matchday
    """,
    [int(selected_team_id)],
    (cache_key(), int(selected_team_id), "pos_history"),
)

if st.session_state.get("debug_mode"):
    st.write("DEBUG pos_history rows", len(pos_history))
    st.write(pos_history.head())
team_matches = query_df(
    """
    select
      tm.match_id,
      tm.match_date,
      tm.matchday,
      tm.is_home,
      tm.opponent_team_id,
      opp.team_name as opponent,
      opp.team_crest_url as opponent_crest_url,
      tm.goals_for,
      tm.goals_against,
      tm.result,
      tm.points
    from fct_team_match tm
    left join stg_raw_teams opp on tm.opponent_team_id = opp.team_id
    where tm.team_id = ?
    order by tm.match_date desc
    """,
    [int(selected_team_id)],
    cache_key(),
)

# Attempt to backfill crest from league table if missing
if (
    selected_team_id is not None
    and not league_table.empty
    and not st.session_state.get("selected_team_crest_url")
):
    row = league_table[league_table["team_id"] == selected_team_id].head(1)
    if not row.empty:
        st.session_state["selected_team_crest_url"] = row.iloc[0].get("team_crest_url")

# -----------------------------------------------------------------------------
# Tabs
# -----------------------------------------------------------------------------
tab_overview, tab_matches, tab_table, tab_about = st.tabs(["Overview", "Matches", "Table", "About"])

# Overview
with tab_overview:
    # KPIs
    latest_pos = pos_history.tail(1)
    kpi_cols = st.columns(5)
    if not latest_pos.empty:
        lp = latest_pos.iloc[0]
        played = int(lp.matchday) if not pd.isna(lp.matchday) else None
        ppg = (lp.points / played) if played and played > 0 else None
        metrics = [
            ("Position", int(lp.position) if not pd.isna(lp.position) else "–"),
            ("Points", int(lp.points) if not pd.isna(lp.points) else "–"),
            ("GD", int(lp.gd) if not pd.isna(lp.gd) else "–"),
            ("Played", played if played is not None else "–"),
            ("PPG", f"{ppg:.2f}" if ppg is not None else "–"),
        ]
    elif not league_table.empty:
        row = league_table[league_table["team_id"] == selected_team_id].head(1)
        if not row.empty:
            r = row.iloc[0]
            metrics = [
                ("Position", int(r.position) if not pd.isna(r.position) else "–"),
                ("Points", int(r.points) if not pd.isna(r.points) else "–"),
                ("GD", int(r.gd) if not pd.isna(r.gd) else "–"),
                ("Played", int(r.played) if not pd.isna(r.played) else "–"),
                ("PPG", f"{r.points / r.played:.2f}" if r.played else "–"),
            ]
        else:
            metrics = []
    else:
        metrics = []

    for col, (label, value) in zip(kpi_cols, metrics or []):
        col.metric(label, value)

    form_col, chart_col = st.columns([1, 1])

    with form_col:
        st.markdown("#### Form")
        if team_matches.empty:
            render_empty("No matches yet for this team.")
        else:
            color_map = {"W": "#22c55e", "D": "#e2e8f0", "L": "#ef4444"}
            tiles = []
            for _, r in team_matches.iterrows():
                res = str(r.get("result", ""))
                bg = color_map.get(res, "#e2e8f0")
                score = (
                    f"{int(r.goals_for)}-{int(r.goals_against)}"
                    if pd.notna(r.goals_for) and pd.notna(r.goals_against)
                    else "—"
                )
                crest = r.get("opponent_crest_url")
                crest_img = (
                    f"<img src='{crest}' style='height:22px;width:22px;vertical-align:middle;border-radius:6px;' />"
                    if crest
                    else ""
                )
                date_str = ""
                if pd.notna(r.match_date):
                    date_str = pd.to_datetime(r.match_date).strftime("%d-%b-%y")
                tiles.append(
                    f"<div style='display:inline-flex;flex-direction:column;align-items:center;justify-content:center;padding:6px 8px;border-radius:10px;background:{bg};color:#0f172a;font-weight:700;width:110px;box-shadow:0 1px 2px rgba(0,0,0,0.06);'>"
                    f"<div style='font-size:0.8rem;color:#0f172a;margin-bottom:2px;'>{date_str}</div>"
                    f"<div style='display:flex;align-items:center;justify-content:space-between;width:100%;gap:6px;'>"
                    f"<div style='flex:0 0 60%;display:flex;align-items:center;justify-content:center;'>{crest_img}</div>"
                    f"<div style='flex:0 0 40%;text-align:center;font-weight:700;'>{res}</div>"
                    f"</div>"
                    f"<div style='font-weight:600;color:#0f172a;margin-top:2px;text-align:center;'>{score}</div>"
                    f"</div>"
                )
            st.markdown(
                f"<div style='display:grid;grid-template-columns:repeat(auto-fit, minmax(110px, 1fr));gap:6px;'>" + "".join(tiles) + "</div>",
                unsafe_allow_html=True,
            )

    with chart_col:
        st.markdown("#### Position through time")
        if pos_history.empty:
            render_empty("Position data not available.")
        else:
            teams_in_league = len(league_table) if not league_table.empty else int(pos_history["position"].max())
            teams_in_league = max(teams_in_league, int(pos_history["position"].max()))
            color_scale = alt.Scale(domain=["W", "D", "L"], range=["#22c55e", "#e2e8f0", "#ef4444"])
            max_md = int(pos_history["matchday"].max()) if not pos_history.empty else 46
            bands = pd.DataFrame(
                [
                    {"y0": 1, "y1": 3, "color": "#16a34a"},    # promotion (1-3)
                    {"y0": 3, "y1": 7, "color": "#86efac"},    # playoffs (3-7)
                    {"y0": 7, "y1": 22, "color": "#e5e7eb"},   # nothing (7-22)
                    {"y0": 22, "y1": 24, "color": "#f87171"},  # relegation (22-24)
                ]
            )
            bands["x0"] = 0
            bands["x1"] = max_md + 1
            band_layer = alt.Chart(bands).mark_rect(opacity=0.6).encode(
                x=alt.X("x0:Q", title=None, scale=alt.Scale(domain=[0, max_md + 1]), axis=alt.Axis(labels=False, ticks=False)),
                x2="x1:Q",
                y="y0:Q",
                y2="y1:Q",
                color=alt.Color("color:N", scale=None, legend=None),
                tooltip=[],
            )

            base = alt.Chart(pos_history).encode(
                x=alt.X("matchday:Q", title="Matchday", scale=alt.Scale(domain=[1, max_md])),
                y=alt.Y(
                    "position:Q",
                    title="Position",
                    scale=alt.Scale(reverse=True, domain=[1, teams_in_league], nice=False, padding=0),
                    axis=alt.Axis(values=list(range(1, teams_in_league + 1))),
                ),
            )
            tooltip_fields = [
                alt.Tooltip("matchday:Q", title="Matchday"),
                alt.Tooltip("position:Q", title="Position"),
                alt.Tooltip("points:Q", title="Points"),
                alt.Tooltip("gd:Q", title="GD"),
                alt.Tooltip("opponent:N", title="Opponent"),
                alt.Tooltip("result:N", title="Result"),
                alt.Tooltip("goals_for:Q", title="Goals For"),
                alt.Tooltip("goals_against:Q", title="Goals Against"),
                alt.Tooltip("as_of_date:T", title="As of"),
            ]
            grid_layer = (
                alt.Chart(pd.DataFrame({"y": list(range(1, teams_in_league + 1))}))
                .mark_rule(color="#cbd5e1", opacity=0.6)
                .encode(y="y:Q", tooltip=[])
            )

            chart = (
                band_layer
                + grid_layer
                + base.mark_line(color="#000000", strokeWidth=3).encode(tooltip=[])
                + base.mark_point(filled=True, size=140, opacity=1).encode(
                    color=alt.Color("result:N", title="Result", scale=color_scale, legend=None),
                    tooltip=tooltip_fields,
                )
            )
            st.altair_chart(chart, use_container_width=True)
            st.caption("Lower is better (1st at the top).")

# Matches tab
with tab_matches:
    if team_matches.empty:
        render_empty("No matches yet for this team.")
    else:
        matches_display = team_matches.copy()
        matches_display["home_away"] = matches_display["is_home"].apply(lambda x: "H" if pd.notna(x) and int(x) == 1 else "A")
        q = st.text_input("Search opponent or matchday").lower().strip()
        filtered = matches_display
        if q:
            filtered = filtered[
                filtered["opponent"].fillna("").str.lower().str.contains(q) | filtered["matchday"].astype(str).str.contains(q)
            ]
        st.dataframe(
            filtered[
                ["match_date", "matchday", "home_away", "opponent", "goals_for", "goals_against", "result", "match_id"]
            ],
            hide_index=True,
            use_container_width=True,
        )
        match_ids = filtered["match_id"].tolist()
        selected_match = st.selectbox("Select a match", match_ids) if match_ids else None

        if selected_match:
            detail = query_df(
                """
                select
                  match_id, utc_date, status, matchday,
                  home_team_name, away_team_name,
                  home_score_full, away_score_full,
                  home_score_half, away_score_half,
                  winner, last_updated_utc
                from stg_raw_matches
                where match_id = ?
                """,
                [int(selected_match)],
                cache_key(),
            )
            if detail.empty:
                render_empty("Match not found.")
            else:
                d = detail.iloc[0]
                st.markdown(
                    f"""
                    <div style="border:1px solid #e2e8f0;border-radius:12px;padding:12px 16px;background:#0b1120;">
                      <div style="color:#cbd5e1;font-weight:600;margin-bottom:4px;">Matchday {d.matchday} · {d.utc_date}</div>
                      <div style="display:flex;justify-content:space-between;align-items:center;font-size:1.2rem;font-weight:700;color:#e2e8f0;">
                        <span>{d.home_team_name}</span>
                        <span>{d.home_score_full} - {d.away_score_full}</span>
                        <span>{d.away_team_name}</span>
                      </div>
                      <div style="color:#94a3b8;margin-top:6px;">HT: {d.home_score_half}-{d.away_score_half} · Status: {d.status}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

# Table tab
with tab_table:
    if league_table.empty:
        render_empty("League table not built yet.")
    else:
        # Build form per team from matches
        form_df = query_df(
            """
            select team_id, result
            from fct_team_match
            order by match_date desc
            """,
            None,
            cache_key(),
        )
        form_map: Dict[int, List[str]] = {}
        if not form_df.empty:
            for tid, group in form_df.groupby("team_id"):
                try:
                    form_map[int(tid)] = group["result"].tolist()[:5]
                except (TypeError, ValueError):
                    continue

        def highlight_team(row: pd.Series) -> List[str]:
            return [
                "background-color: #e0f2ff; color: #0f172a"
                if row.get("team_id") is not None
                and selected_team_id is not None
                and str(row.get("team_id")) == str(selected_team_id)
                else ""
                for _ in row
            ]

        table_display = league_table.copy()
        table_display["Form"] = table_display["team_id"].apply(
            lambda tid: table_form_badges(form_map.get(int(tid), [])) if pd.notna(tid) else ""
        )
        st.dataframe(
            table_display[
                ["position", "team_name", "played", "won", "drawn", "lost", "gf", "ga", "gd", "points", "Form"]
            ].style.apply(highlight_team, axis=1).format({"Form": lambda x: x}),
            hide_index=True,
            use_container_width=True,
        )

# About tab
with tab_about:
    st.markdown(
        """
        **Data source:** football-data.org (fixtures/results) → DuckDB → dbt marts → Streamlit.

        **Known limitations (v1):** No xG, shots, cards, or player events. Standings based on completed matches only.

        **Usage:** Select a team in the sidebar, run refresh to ingest + rebuild marts, then explore Overview, Matches, and Table tabs.
        """,
    )
