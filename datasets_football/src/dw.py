from __future__ import annotations

from pathlib import Path
import duckdb


def build_dw(db_path: Path) -> None:
    """
    Build a simple star schema in DuckDB:
    - dim_team
    - dim_player
    - dim_date
    - fact_match
    - fact_player_match
    """
    con = duckdb.connect(str(db_path))

    # DIM: team
    con.execute("""
        CREATE OR REPLACE TABLE dim_team AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY team_name) AS team_sk,
            team_name
        FROM (
            SELECT DISTINCT home_team AS team_name FROM stg_matches
            UNION
            SELECT DISTINCT away_team AS team_name FROM stg_matches
            UNION
            SELECT DISTINCT team AS team_name FROM stg_player_stats
        )
        WHERE team_name IS NOT NULL AND team_name <> ''
    """)

    # DIM: player (player_id as natural key)
    con.execute("""
        CREATE OR REPLACE TABLE dim_player AS
        SELECT
            player_id,
            ANY_VALUE(player_name) AS player_name,
            ANY_VALUE(position) AS position
        FROM stg_player_stats
        WHERE player_id IS NOT NULL AND player_id <> ''
        GROUP BY player_id
    """)

    # DIM: date
    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            date AS match_date,
            EXTRACT(year FROM date) AS year,
            EXTRACT(month FROM date) AS month,
            EXTRACT(day FROM date) AS day,
            STRFTIME(date, '%w') AS day_of_week_num
        FROM stg_matches
        WHERE date IS NOT NULL
    """)

    # FACT: match
    con.execute("""
        CREATE OR REPLACE TABLE fact_match AS
        SELECT
            m.match_id,
            m.season,
            m.league,
            m.date AS match_date,
            dt.year,
            dt.month,
            m.stadium,
            m.referee,
            m.attendance,
            home_t.team_sk AS home_team_sk,
            away_t.team_sk AS away_team_sk,
            m.home_goals,
            m.away_goals,
            m.home_shots,
            m.away_shots,
            m.home_xG,
            m.away_xG,
            m.home_possession_pct,
            m.away_possession_pct,
            CASE
                WHEN m.home_goals > m.away_goals THEN 'H'
                WHEN m.home_goals < m.away_goals THEN 'A'
                ELSE 'D'
            END AS result
        FROM stg_matches m
        LEFT JOIN dim_date dt ON dt.match_date = m.date
        LEFT JOIN dim_team home_t ON home_t.team_name = m.home_team
        LEFT JOIN dim_team away_t ON away_t.team_name = m.away_team
    """)

    # FACT: player-match (player stats per match)
    con.execute("""
        CREATE OR REPLACE TABLE fact_player_match AS
        SELECT
            s.match_id,
            fm.match_date,
            s.player_id,
            p.player_name,
            t.team_sk AS team_sk,
            s.team AS team_name,
            s.position,
            s.minutes,
            s.shots,
            s.goals,
            s.assists,
            s.passes,
            s.pass_accuracy_pct,
            s.tackles,
            s.interceptions,
            s.fouls_committed,
            NULLIF(s.card, 'nan') AS card,
            s.rating
        FROM stg_player_stats s
        LEFT JOIN dim_player p ON p.player_id = s.player_id
        LEFT JOIN dim_team t ON t.team_name = s.team
        LEFT JOIN fact_match fm ON fm.match_id = s.match_id
    """)

    con.close()