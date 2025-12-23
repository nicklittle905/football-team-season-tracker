with src as (
  select
    team_id,
    name as team_name,
    short_name,
    tla,
    coalesce(crest_url, crest) as team_crest_url,
    fetched_at_utc
  from {{ source('raw', 'raw_teams') }}
)
select * from src
