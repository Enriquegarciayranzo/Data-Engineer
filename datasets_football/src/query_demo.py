from __future__ import annotations
from pathlib import Path
import duckdb

# Resolve project base directory and DuckDB database path
BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "gold" / "football_dw.duckdb"

def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    # Display aggregated KPIs
    print("\nGOLD KPIs")
    print(con.execute("SELECT * FROM gold_kpis ORDER BY kpi").df())

    # Show top players by average rating
    print("\nTOP PLAYERS BY RATING")
    print(con.execute("SELECT * FROM vw_top_players_rating LIMIT 10").df())

    # Compare goals scored vs expected goals (xG) by team
    print("\nTEAMS: GOALS VS xG")
    print(con.execute("SELECT * FROM vw_team_goals_vs_xg LIMIT 10").df())

    # Display league table based on points
    print("\nLEAGUE TABLE (POINTS)")
    print(con.execute("SELECT * FROM vw_league_table_points LIMIT 10").df())

    # Show match-level performance differentials
    print("\nWIN DRIVERS (DELTAS)")
    print(con.execute("SELECT * FROM vw_win_drivers_deltas LIMIT 10").df())

    # Display defensive intensity metrics by team
    print("\nDEFENSIVE INTENSITY BY TEAM")
    print(con.execute("SELECT * FROM vw_team_defensive_intensity LIMIT 10").df())

    con.close()

if __name__ == "__main__":
    main()