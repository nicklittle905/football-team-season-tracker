from pathlib import Path
from typing import Any, List, Optional, Tuple

import altair as alt
import duckdb
import pandas as pd
import streamlit as st
from datetime import datetime

from pipeline.run_pipeline import run_refresh

DB_PATH = Path(__file__).resolve().parent / "warehouse/charlton.duckdb"
DEFAULT_TEAM_ID = 348
DEFAULT_TEAM_NAME = "Charlton Athletic FC"

st.set_page_config(page_title="Charlton Season Tracker", layout="wide")
st.title("Charlton Athletic — Season Tracker")

session_defaults = {
    "selected_team_id": DEFAULT_TEAM_ID,
    "refreshing": False,
    "refresh_result": None,
    "last_refreshed_team_id": None,
    "last_refreshed_ts": None,
    "last_refresh_duration": None,
}
for key, value in session_defaults.items():
    st.session_state.setdefault(key, value)


def safe_query_df(sql: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
    """Run a query; return empty DataFrame if DB missing or table absent."""
    if not DB_PATH.exists():
        return pd.DataFrame()
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            return con.execute(sql, params or []).df()
    except duckdb.Error:
        return pd.DataFrame()


def safe_query_scalar(sql: str, params: Optional[List[Any]] = None) -> Optional[Any]:
    df = safe_query_df(sql, params)
    if df.empty:
        return None
    return df.iloc[0, 0]


def fetch_team_options() -> List[Tuple[int, str]]:
    """Fetch team_id + name from stg_raw_teams (fallback raw_teams)."""
    if not DB_PATH.exists():
        return []
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        for table in ("stg_raw_teams", "raw_teams"):
            try:
                rows = con.execute(f"select team_id, team_name from {table} order by team_name").fetchall()
                if rows:
                    return [(int(r[0]), str(r[1])) for r in rows]
            except duckdb.Error:
                continue
    return []


if not DB_PATH.exists():
    st.warning(f"Database not found at {DB_PATH}. Click Refresh data to ingest + build.")

teams = fetch_team_options()

team_idx = 0
selected_team_name = DEFAULT_TEAM_NAME
if teams:
    for i, (tid, name) in enumerate(teams):
        if tid == st.session_state["selected_team_id"]:
            team_idx = i
            selected_team_name = name
            break
    selected_team_id, selected_team_name = st.selectbox(
        "Select team",
        options=teams,
        index=team_idx,
        format_func=lambda opt: opt[1],
    )
    st.session_state["selected_team_id"] = selected_team_id
else:
    selected_team_id = st.session_state["selected_team_id"]
    st.info("No teams found yet. Run refresh to ingest data.")

controls_col, status_col = st.columns([2, 1])

with controls_col:
    refresh_disabled = st.session_state["refreshing"]
    if st.button(
        "Refresh data",
        type="primary",
        disabled=refresh_disabled,
        help="Runs ingest + dbt for the selected team_id",
    ):
        st.session_state["refreshing"] = True
        with st.spinner(f"Running ingest + dbt for team_id {selected_team_id}…"):
            start_ts = datetime.utcnow()
            try:
                result = run_refresh(int(selected_team_id))
            except Exception as exc:  # noqa: BLE001
                result = {
                    "ingest_ok": False,
                    "dbt_ok": False,
                    "ingest_stdout": str(exc),
                    "dbt_stdout": "",
                }
            duration = (datetime.utcnow() - start_ts).total_seconds()
            st.session_state["refresh_result"] = result
            if result.get("ingest_ok") and result.get("dbt_ok"):
                st.session_state["last_refreshed_team_id"] = selected_team_id
                st.session_state["last_refreshed_ts"] = datetime.utcnow()
                st.session_state["last_refresh_duration"] = duration
                st.success("Ingest + dbt completed.")
            else:
                st.error("Refresh failed.")
                with st.expander("See logs"):
                    st.write(result)
        st.session_state["refreshing"] = False

    data_team_id = st.session_state.get("last_refreshed_team_id")
    if data_team_id and data_team_id != selected_team_id:
        st.warning(f"Data currently reflects team_id {data_team_id}. Click Refresh to update selection.")

    if st.session_state.get("last_refreshed_ts"):
        ts = st.session_state["last_refreshed_ts"].strftime("%Y-%m-%d %H:%M:%S UTC")
        dur = st.session_state.get("last_refresh_duration")
        dur_str = f"{dur:.1f}s" if dur is not None else ""
        st.caption(f"Last refresh: {ts} {f'({dur_str})' if dur_str else ''}")

with status_col:
    st.subheader("Status")
    latest_ingest = safe_query_df(
        """
        select run_ts_utc, status, details
        from ingest_runs
        order by run_ts_utc desc
        limit 1
        """
    )
    if not latest_ingest.empty:
        row = latest_ingest.iloc[0]
        st.write(f"Ingest: **{row.status}** at {row.run_ts_utc}")
        if row.details:
            st.caption(row.details)
    else:
        st.caption("No ingest_runs yet.")

    counts = {
        "mart_league_table_current": safe_query_scalar("select count(*) from mart_league_table_current"),
        "mart_team_position_through_time": safe_query_scalar("select count(*) from mart_team_position_through_time"),
        "mart_team_last_5": safe_query_scalar("select count(*) from mart_team_last_5"),
    }
    st.write("Rowcounts:")
    for name, cnt in counts.items():
        st.caption(f"- {name}: {cnt if cnt is not None else 'n/a'}")

st.divider()

league_table = safe_query_df("select * from mart_league_table_current order by position")
pos = safe_query_df(
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
)
last5 = safe_query_df("select * from mart_team_last_5 order by match_date desc")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Current league table")
    if league_table.empty:
        st.info("League table not available yet. Run refresh.")
    else:
        def highlight_team(row: pd.Series) -> List[str]:
            return [
                "background-color: #e0f2ff; color: #0f172a"
                if str(row.get("team_name")) == str(selected_team_name)
                else ""
                for _ in row
            ]

        styled_table = league_table.style.apply(highlight_team, axis=1)
        st.dataframe(styled_table, use_container_width=True, hide_index=True)

    st.subheader("Next fixtures")

    def team_form(team_id: int, n: int = 5) -> List[str]:
        df = safe_query_df(
            """
            select result
            from fct_team_match
            where team_id = ?
            order by match_date desc
            limit ?
            """,
            [team_id, n],
        )
        return df["result"].tolist() if not df.empty else []

    def form_badges(results: List[str]) -> str:
        colors = {"W": "#22c55e", "D": "#e2e8f0", "L": "#ef4444"}
        tags = []
        for r in results:
            bg = colors.get(r, "#e2e8f0")
            tags.append(
                f'<span style="display:inline-block;padding:4px 8px;margin-right:4px;border-radius:999px;background:{bg};color:#0f172a;font-weight:600;">{r}</span>'
            )
        return "".join(tags) if tags else '<span style="color:#94a3b8;">no form</span>'

    fixtures = safe_query_df(
        """
        select
          m.match_id,
          m.matchday,
          m.utc_date,
          m.home_team_id,
          m.away_team_id,
          h.team_name as home_team,
          a.team_name as away_team
        from stg_raw_matches m
        left join stg_raw_teams h on m.home_team_id = h.team_id
        left join stg_raw_teams a on m.away_team_id = a.team_id
        where m.status in ('SCHEDULED', 'TIMED')
          and (m.home_team_id = ? or m.away_team_id = ?)
        order by m.utc_date
        limit 5
        """,
        [int(selected_team_id), int(selected_team_id)],
    )

    # Map team_id -> position for quick lookup
    positions = {int(r["team_id"]): int(r["position"]) for _, r in league_table.iterrows()} if not league_table.empty else {}

    def predict(home_id: Optional[int], away_id: Optional[int]) -> str:
        hp, ap = positions.get(home_id), positions.get(away_id)
        if hp is None or ap is None:
            return "Draw"
        if abs(hp - ap) <= 1:
            return "Draw"
        return "Home win" if hp < ap else "Away win"

    if fixtures.empty:
        st.info("No upcoming fixtures found.")
    else:
        for _, f in fixtures.iterrows():
            home_form = form_badges(team_form(int(f.home_team_id))) if pd.notna(f.home_team_id) else ""
            away_form = form_badges(team_form(int(f.away_team_id))) if pd.notna(f.away_team_id) else ""
            prediction = predict(int(f.home_team_id) if pd.notna(f.home_team_id) else None, int(f.away_team_id) if pd.notna(f.away_team_id) else None)
            kickoff = pd.to_datetime(f.utc_date).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(f.utc_date) else "TBD"

            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:12px;padding:12px 16px;margin-bottom:12px;background:#0b1120;">
                  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                    <span style="color:#cbd5e1;font-weight:600;">Matchday {int(f.matchday) if pd.notna(f.matchday) else '—'} · {kickoff}</span>
                    <span style="background:#e0f2ff;color:#0f172a;padding:4px 10px;border-radius:999px;font-weight:700;">{prediction}</span>
                  </div>
                  <div style="display:flex;align-items:center;gap:12px;">
                    <div style="flex:1;text-align:right;">
                      <div style="color:#e2e8f0;font-size:1.05rem;font-weight:700;">{f.home_team or 'TBD'}</div>
                      <div style="margin-top:6px;">{home_form}</div>
                    </div>
                    <div style="width:60px;text-align:center;color:#cbd5e1;font-weight:600;">vs</div>
                    <div style="flex:1;text-align:left;">
                      <div style="color:#e2e8f0;font-size:1.05rem;font-weight:700;">{f.away_team or 'TBD'}</div>
                      <div style="margin-top:6px;">{away_form}</div>
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

with col2:
    st.subheader(f"{selected_team_name} position through time")
    if not pos.empty:
        teams_in_league = len(league_table) if not league_table.empty else int(pos["position"].max())
        teams_in_league = max(teams_in_league, int(pos["position"].max()))
        color_scale = alt.Scale(domain=["W", "D", "L"], range=["#22c55e", "#e2e8f0", "#ef4444"])

        base = alt.Chart(pos).encode(
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

        line = base.mark_line(color="#cbd5e1", strokeWidth=3)
        points = base.mark_point(filled=True, size=140, strokeWidth=0).encode(
            color=alt.Color("result:N", title="Result", scale=color_scale, legend=None)
        )
        chart = line + points
        st.altair_chart(chart, use_container_width=True)
        st.caption("Lower is better (1st at the top). Y-axis reversed.")
    else:
        st.info("Position data not available. Run refresh for the selected team.")

st.subheader(f"{selected_team_name} matches")
if last5.empty:
    st.info("No matches found. Run refresh.")
else:
    # Form strip (last 5 results)
    recent_form = last5.head(5)
    form_colors = {"W": "#22c55e", "D": "#e2e8f0", "L": "#ef4444"}
    form_text = {"W": "W", "D": "D", "L": "L"}
    form_row = []
    for _, r in recent_form.iterrows():
        res = str(r.get("result"))
        bg = form_colors.get(res, "#e2e8f0")
        label = form_text.get(res, res)
        form_row.append(
            f'<span style="display:inline-block;padding:6px 10px;margin-right:6px;border-radius:8px;background:{bg};color:#0f172a;font-weight:600;">{label}</span>'
        )
    st.markdown("Form (last 5): " + "".join(form_row), unsafe_allow_html=True)

    st.dataframe(last5, use_container_width=True, hide_index=True)

st.subheader("Match detail")
match_ids = last5["match_id"].tolist() if not last5.empty else []
selected_match = st.selectbox("Select a match", match_ids)

if selected_match:
    detail = safe_query_df(
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
    )
    if detail.empty:
        st.info("Match not found. Rebuild data if needed.")
    else:
        st.dataframe(detail, use_container_width=True, hide_index=True)
