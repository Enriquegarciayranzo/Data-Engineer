from __future__ import annotations

from pathlib import Path
import duckdb


def build_kpis(db_path: Path) -> None:
    """
    Build a simple KPI table in the gold layer.
    """
    con = duckdb.connect(str(db_path))

    con.execute("CREATE OR REPLACE TABLE gold_kpis(kpi VARCHAR, value DOUBLE)")

    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_total_goals_per_match', AVG(home_goals + away_goals)::DOUBLE
        FROM fact_match
        WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
    """)

    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_attendance', AVG(attendance)::DOUBLE
        FROM fact_match
        WHERE attendance IS NOT NULL
    """)

    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_player_rating', AVG(rating)::DOUBLE
        FROM fact_player_match
        WHERE rating IS NOT NULL
    """)

    con.execute("""
        INSERT INTO gold_kpis
        SELECT 'avg_minutes_played', AVG(minutes)::DOUBLE
        FROM fact_player_match
        WHERE minutes IS NOT NULL
    """)

    con.close()