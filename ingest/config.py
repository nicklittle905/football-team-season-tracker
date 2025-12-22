from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    token: str
    comp_code: str
    season: int
    team_name: str
    duckdb_path: str

def load_settings() -> Settings:
    token = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
    if not token:
        raise RuntimeError("FOOTBALL_DATA_TOKEN is missing. Put it in your .env or environment.")

    comp_code = os.getenv("COMP_CODE", "ELC").strip()  # Championship
    season = int(os.getenv("SEASON", "2025"))
    team_name = os.getenv("TEAM_NAME", "Charlton Athletic").strip()
    duckdb_path = os.getenv("DUCKDB_PATH", "warehouse/charlton.duckdb").strip()

    return Settings(
        token=token,
        comp_code=comp_code,
        season=season,
        team_name=team_name,
        duckdb_path=duckdb_path,
    )