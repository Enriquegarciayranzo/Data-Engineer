from __future__ import annotations
from pathlib import Path
import duckdb

def build_business_views(db_path: Path) -> None:
    """
    Create business-friendly views for analytics.
    """
    con = duckdb.connect(str(db_path))

    # 1) Top players by rating
    con.execute("""
        CREATE OR REPLACE VIEW vw_top_players_rating AS
        SELECT
            player_id,
            player_name,
            position,
            team_name,
            COUNT(*) AS matches_played,
            AVG(minutes) AS avg_minutes,
            AVG(rating) AS avg_rating
        FROM fact_player_match
        WHERE rating IS NOT NULL AND minutes IS NOT NULL
        GROUP BY player_id, player_name, position, team_name
        HAVING COUNT(*) >= 1
        ORDER BY avg_rating DESC, matches_played DESC
    """)

    # 2) Teams: goals vs xG
    con.execute("""
        CREATE OR REPLACE VIEW vw_team_goals_vs_xg AS
        WITH team_match AS (
            SELECT
                home_team_sk AS team_sk,
                (home_goals)::DOUBLE AS goals,
                (home_xG)::DOUBLE AS xg
            FROM fact_match
            WHERE home_goals IS NOT NULL AND home_xG IS NOT NULL

            UNION ALL

            SELECT
                away_team_sk AS team_sk,
                (away_goals)::DOUBLE AS goals,
                (away_xG)::DOUBLE AS xg
            FROM fact_match
            WHERE away_goals IS NOT NULL AND away_xG IS NOT NULL
        )
        SELECT
            t.team_name,
            COUNT(*) AS matches,
            AVG(goals) AS avg_goals,
            AVG(xg) AS avg_xg,
            (AVG(goals) - AVG(xg)) AS goals_minus_xg
        FROM team_match tm
        JOIN dim_team t ON t.team_sk = tm.team_sk
        GROUP BY t.team_name
        HAVING COUNT(*) >= 10
        ORDER BY goals_minus_xg DESC, matches DESC
    """)

    # 3) League table (points)
    con.execute("""
        CREATE OR REPLACE VIEW vw_league_table_points AS
        WITH home AS (
            SELECT
                home_team_sk AS team_sk,
                CASE
                    WHEN result = 'H' THEN 3
                    WHEN result = 'D' THEN 1
                    ELSE 0
                END AS pts,
                home_goals AS gf,
                away_goals AS ga
            FROM fact_match
            WHERE result IS NOT NULL
        ),
        away AS (
            SELECT
                away_team_sk AS team_sk,
                CASE
                    WHEN result = 'A' THEN 3
                    WHEN result = 'D' THEN 1
                    ELSE 0
                END AS pts,
                away_goals AS gf,
                home_goals AS ga
            FROM fact_match
            WHERE result IS NOT NULL
        ),
        allm AS (
            SELECT * FROM home
            UNION ALL
            SELECT * FROM away
        )
        SELECT
            t.team_name,
            COUNT(*) AS matches,
            SUM(pts) AS points,
            SUM(gf) AS goals_for,
            SUM(ga) AS goals_against,
            (SUM(gf) - SUM(ga)) AS goal_diff
        FROM allm a
        JOIN dim_team t ON t.team_sk = a.team_sk
        GROUP BY t.team_name
        ORDER BY points DESC, goal_diff DESC, goals_for DESC
    """)

    # 4) Match deltas (potential win drivers)
    con.execute("""
        CREATE OR REPLACE VIEW vw_win_drivers_deltas AS
        SELECT
            fm.match_id,
            home_t.team_name || ' vs ' || away_t.team_name AS game,
            fm.result,
            (fm.home_xG - fm.away_xG) AS delta_xg,
            (fm.home_shots - fm.away_shots) AS delta_shots,
            (fm.home_possession_pct - fm.away_possession_pct) AS delta_possession
        FROM fact_match fm
        JOIN dim_team home_t ON home_t.team_sk = fm.home_team_sk
        JOIN dim_team away_t ON away_t.team_sk = fm.away_team_sk
        WHERE
            fm.home_xG IS NOT NULL AND fm.away_xG IS NOT NULL
            AND fm.home_shots IS NOT NULL AND fm.away_shots IS NOT NULL
            AND fm.home_possession_pct IS NOT NULL AND fm.away_possession_pct IS NOT NULL
            AND fm.result IS NOT NULL
    """)

    # 5) Defensive intensity by team (tackles+interceptions per 90)
    con.execute("""
        CREATE OR REPLACE VIEW vw_team_defensive_intensity AS
        SELECT
            team_name,
            COUNT(*) AS player_match_rows,
            SUM(tackles + interceptions) AS total_actions,
            SUM(minutes) AS total_minutes,
            (SUM(tackles + interceptions) / NULLIF(SUM(minutes), 0)) * 90.0 AS actions_per_90
        FROM fact_player_match
        WHERE tackles IS NOT NULL AND interceptions IS NOT NULL AND minutes IS NOT NULL
        GROUP BY team_name
        HAVING SUM(minutes) > 0
        ORDER BY actions_per_90 DESC
    """)

    con.close()