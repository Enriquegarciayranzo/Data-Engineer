from __future__ import annotations
from pathlib import Path
import duckdb

def build_kpis(db_path: Path) -> None:
    # Connect to the analytical DuckDB database
    con = duckdb.connect(str(db_path))

    # Create KPI table (idempotent)
    con.execute("CREATE OR REPLACE TABLE gold_kpis(kpi VARCHAR, value DOUBLE)")

    # Create KPI table (idempotent)
    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_total_goals_per_match', AVG(home_goals + away_goals)::DOUBLE
        FROM fact_match
        WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
    """)

    # Average total goals per match
    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_attendance', AVG(attendance)::DOUBLE
        FROM fact_match
        WHERE attendance IS NOT NULL
    """)

    # Average player performance rating
    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_player_rating', AVG(rating)::DOUBLE
        FROM fact_player_match
        WHERE rating IS NOT NULL
    """)

    # Average minutes played per player-match
    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_minutes_played', AVG(minutes)::DOUBLE
        FROM fact_player_match
        WHERE minutes IS NOT NULL
    """)

    con.close()