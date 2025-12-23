from datetime import datetime
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from pipeline.run_pipeline import run_refresh

DB_PATH = Path(__file__).resolve().parent / "warehouse/charlton.duckdb"
DEFAULT_TEAM_ID = 348
DEFAULT_TEAM_NAME = "Charlton Athletic FC"
COMP_CODE = os.getenv("COMP_CODE", "ELC")
SEASON = os.getenv("SEASON", "2025")

st.set_page_config(page_title="Season Tracker", layout="wide")

# -----------------------------------------------------------------------------
# Session + caching helpers
# -----------------------------------------------------------------------------
session_defaults = {
    "selected_team_id": DEFAULT_TEAM_ID,
    "selected_team_name": DEFAULT_TEAM_NAME,
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
    return int(st.session_state.get("cache_buster", 0))


@st.cache_data(show_spinner=False)
def query_df(sql: str, params: Optional[List[Any]], cache_seed: int) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            return con.execute(sql, params or []).df()
    except duckdb.Error:
        return pd.DataFrame()


def invalidate_cache() -> None:
    st.session_state["cache_buster"] = st.session_state.get("cache_buster", 0) + 1


def fetch_teams() -> List[Tuple[int, str]]:
    for table in ("stg_raw_teams", "raw_teams"):
        df = query_df(f"select team_id, team_name from {table} order by team_name", None, cache_key())
        if not df.empty:
            return [(int(r.team_id), str(r.team_name)) for _, r in df.iterrows()]
    return []


def form_chips(results: List[str]) -> str:
    colors = {"W": "#22c55e", "D": "#e2e8f0", "L": "#ef4444"}
    return "".join(
        f'<span style="display:inline-block;padding:4px 10px;margin-right:6px;border-radius:999px;background:{colors.get(r, "#e2e8f0")};color:#0f172a;font-weight:700;">{r}</span>'
        for r in results
    )


def render_empty(message: str) -> None:
    st.info(message + " Click Refresh to build data.")


# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Season Tracker")
    st.caption(f"Competition: **{COMP_CODE}** · Season: **{SEASON}**")

    teams = fetch_teams()
    selected_team_id = st.session_state["selected_team_id"]
    selected_team_name = st.session_state["selected_team_name"]

    if teams:
        options = teams
        try:
            idx = next(i for i, t in enumerate(options) if t[0] == selected_team_id)
        except StopIteration:
            idx = 0
        selected_team_id, selected_team_name = st.selectbox(
            "Team",
            options=options,
            index=idx,
            format_func=lambda opt: opt[1],
        )
        st.session_state["selected_team_id"] = selected_team_id
        st.session_state["selected_team_name"] = selected_team_name
    else:
        st.warning("No teams loaded yet.")

    col_btn, col_dbg = st.columns([3, 1])
    with col_btn:
        if st.button("Refresh pipeline", type="primary", disabled=st.session_state["refreshing"]):
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

# -----------------------------------------------------------------------------
# Data pulls
# -----------------------------------------------------------------------------
league_table = query_df("select * from mart_league_table_current order by position", None, cache_key())
pos_history = query_df(
    """
    select
      s.matchday,
      s.as_of_date,
      s.position,
      s.points,
      s.gd,
      tm.result,
      tm.goals_for,
      tm.goals_against,
      opp.team_name as opponent
    from mart_team_position_through_time s
    left join fct_team_match tm
      on tm.matchday = s.matchday
     and tm.team_id = ?
    left join stg_raw_teams opp
      on tm.opponent_team_id = opp.team_id
    order by s.matchday
    """,
    [int(selected_team_id)],
    cache_key(),
)
team_matches = query_df("select * from mart_team_last_5 order by match_date desc", None, cache_key())

# -----------------------------------------------------------------------------
# Header
# -----------------------------------------------------------------------------
st.markdown(
    f"""
    <div style="display:flex;justify-content:space-between;align-items:flex-end;padding:8px 4px 0;">
      <div>
        <div style="color:#94a3b8;font-size:0.9rem;">{COMP_CODE} · {SEASON}</div>
        <div style="font-size:2rem;font-weight:800;">{selected_team_name}</div>
      </div>
      <div style="color:#94a3b8;font-size:0.9rem;">Team ID: {selected_team_id}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

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
            ("Position", lp.position),
            ("Points", lp.points),
            ("GD", lp.gd),
            ("Played", played),
            ("PPG", f"{ppg:.2f}" if ppg is not None else "–"),
        ]
    elif not league_table.empty:
        row = league_table[league_table["team_id"] == selected_team_id].head(1)
        if not row.empty:
            r = row.iloc[0]
            metrics = [
                ("Position", r.position),
                ("Points", r.points),
                ("GD", r.gd),
                ("Played", r.played),
                ("PPG", f"{r.points / r.played:.2f}" if r.played else "–"),
            ]
        else:
            metrics = []
    else:
        metrics = []

    for col, (label, value) in zip(kpi_cols, metrics or []):
        col.metric(label, value)

    st.markdown("#### Form")
    if team_matches.empty:
        render_empty("No matches yet for this team.")
    else:
        recent = team_matches.head(5)
        st.markdown(form_chips(recent["result"].tolist()), unsafe_allow_html=True)
        st.dataframe(
            recent[["match_date", "matchday", "home_away", "opponent", "goals_for", "goals_against", "result"]],
            hide_index=True,
            use_container_width=True,
        )

    st.markdown("#### Position through time")
    if pos_history.empty:
        render_empty("Position data not available.")
    else:
        teams_in_league = len(league_table) if not league_table.empty else int(pos_history["position"].max())
        teams_in_league = max(teams_in_league, int(pos_history["position"].max()))
        color_scale = alt.Scale(domain=["W", "D", "L"], range=["#22c55e", "#e2e8f0", "#ef4444"])
        base = alt.Chart(pos_history).encode(
            x=alt.X("matchday:Q", title="Matchday"),
            y=alt.Y(
                "position:Q",
                title="Position",
                scale=alt.Scale(reverse=True, domain=[1, teams_in_league]),
                axis=alt.Axis(values=list(range(1, teams_in_league + 1))),
            ),
            tooltip=[
                alt.Tooltip("matchday:Q", title="Matchday"),
                alt.Tooltip("position:Q", title="Position"),
                alt.Tooltip("points:Q", title="Points"),
                alt.Tooltip("gd:Q", title="GD"),
                alt.Tooltip("opponent:N", title="Opponent"),
                alt.Tooltip("result:N", title="Result"),
                alt.Tooltip("goals_for:Q", title="Goals For"),
                alt.Tooltip("goals_against:Q", title="Goals Against"),
                alt.Tooltip("as_of_date:T", title="As of"),
            ],
        )
        chart = base.mark_line(color="#cbd5e1", strokeWidth=3) + base.mark_point(
            filled=True, size=140, strokeWidth=0
        ).encode(color=alt.Color("result:N", title="Result", scale=color_scale, legend=None))
        st.altair_chart(chart, use_container_width=True)
        st.caption("Lower is better (1st at the top).")

# Matches tab
with tab_matches:
    if team_matches.empty:
        render_empty("No matches yet for this team.")
    else:
        q = st.text_input("Search opponent or matchday").lower().strip()
        filtered = team_matches
        if q:
            filtered = filtered[
                filtered["opponent"].str.lower().str.contains(q) | filtered["matchday"].astype(str).str.contains(q)
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
            lambda tid: form_chips(form_map.get(int(tid), [])) if pd.notna(tid) else ""
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
