{{ config(materialized='view') }}

with latest as (
  select
    competition_code,
    season_start_year,
    max(matchday) as latest_matchday
  from {{ ref('fct_standings_matchday') }}
  group by 1,2
)

select
  s.team_id,
  s.position,
  t.team_name,
  s.played, s.won, s.drawn, s.lost,
  s.gf, s.ga, s.gd,
  s.points,
  s.matchday,
  s.last_match_date
from {{ ref('fct_standings_matchday') }} s
join latest l
  on s.competition_code = l.competition_code
 and s.season_start_year = l.season_start_year
 and s.matchday = l.latest_matchday
left join {{ ref('stg_raw_teams') }} t
  on s.team_id = t.team_id
order by s.position
