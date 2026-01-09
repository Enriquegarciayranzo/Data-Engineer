from __future__ import annotations

from pathlib import Path
import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "gold" / "football_dw.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    print("\nGOLD KPIs")
    print(con.execute("SELECT * FROM gold_kpis ORDER BY kpi").df())

    print("\nTOP PLAYERS BY RATING")
    print(con.execute("SELECT * FROM vw_top_players_rating LIMIT 10").df())

    print("\nTEAMS: GOALS VS xG")
    print(con.execute("SELECT * FROM vw_team_goals_vs_xg LIMIT 10").df())

    print("\nLEAGUE TABLE (POINTS)")
    print(con.execute("SELECT * FROM vw_league_table_points LIMIT 10").df())

    print("\nWIN DRIVERS (DELTAS)")
    print(con.execute("SELECT * FROM vw_win_drivers_deltas LIMIT 10").df())

    print("\nDEFENSIVE INTENSITY BY TEAM")
    print(con.execute("SELECT * FROM vw_team_defensive_intensity LIMIT 10").df())

    con.close()


if __name__ == "__main__":
    main()