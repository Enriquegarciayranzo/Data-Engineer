import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "gold" / "football_dw.duckdb"

def main():
    con = duckdb.connect(str(DB_PATH))

    # ----------------------------
    # BUSINESS QUERIES
    # ----------------------------
    print("\nTOP PLAYERS BY RATING")
    print(con.execute("""
        SELECT
            player_id,
            player_name,
            position,
            team_name,
            COUNT(*) AS matches_played,
            AVG(minutes) AS avg_minutes,
            AVG(rating) AS avg_rating
        FROM fact_player_match
        GROUP BY player_id, player_name, position, team_name
        ORDER BY avg_rating DESC
        LIMIT 10
    """).df())

    print("\nTEAMS: GOALS VS EXPECTED GOALS (xG)")
    print(con.execute("""
        SELECT *
        FROM vw_team_goals_vs_xg
        LIMIT 10
    """).df())

    print("\nLEAGUE TABLE (POINTS)")
    print(con.execute("""
        SELECT *
        FROM vw_league_table_points
        LIMIT 10
    """).df())

    print("\nMATCH PERFORMANCE DELTAS")
    print(con.execute("""
        SELECT
            game,
            result,
            delta_xg,
            delta_shots,
            delta_possession
        FROM vw_win_drivers_deltas
        LIMIT 10
    """).df())

    print("\nDEFENSIVE INTENSITY BY TEAM")
    print(con.execute("""
        SELECT *
        FROM vw_team_defensive_intensity
        LIMIT 10
    """).df())

    con.close()

if __name__ == "__main__":
    main()