# src/pipeline.py
from __future__ import annotations

from pathlib import Path
import logging
import sys
import pandas as pd
import duckdb


# ----------------------------
# Paths
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE = BASE_DIR / "data" / "bronze"
GOLD = BASE_DIR / "data" / "gold"
DB_PATH = GOLD / "football_dw.duckdb"

MATCHES_CSV = BRONZE / "futbol_matches.csv"
STATS_CSV = BRONZE / "futbol_player_stats.csv"


# ----------------------------
# Logging
# ----------------------------
def setup_logging() -> None:
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ----------------------------
# Extract
# ----------------------------
def extract_matches(path: Path) -> pd.DataFrame:
    # matches suele venir en latin1; si no, probamos utf-8
    try:
        return pd.read_csv(path, encoding="latin1")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")


def extract_player_stats(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")


# ----------------------------
# Transform (Silver)
# ----------------------------
def transform(matches: pd.DataFrame, stats: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = matches.copy()
    s = stats.copy()

    # Normaliza nombres de columnas por si hubiera espacios raros
    m.columns = [c.strip() for c in m.columns]
    s.columns = [c.strip() for c in s.columns]

    # Parse fecha
    # En tu dataset la columna es "date"
    if "date" in m.columns:
        m["date"] = pd.to_datetime(m["date"], errors="coerce")

    # Tipos numéricos (robusto: solo si existen)
    numeric_cols_matches = [
        "home_goals", "away_goals", "home_shots", "away_shots",
        "home_xG", "away_xG", "home_possession_pct", "away_possession_pct",
        "attendance",
    ]
    for col in numeric_cols_matches:
        if col in m.columns:
            m[col] = pd.to_numeric(m[col], errors="coerce")

    numeric_cols_stats = [
        "minutes", "shots", "goals", "assists", "passes",
        "pass_accuracy_pct", "tackles", "interceptions",
        "fouls_committed", "rating",
    ]
    for col in numeric_cols_stats:
        if col in s.columns:
            s[col] = pd.to_numeric(s[col], errors="coerce")

    # Limpieza básica strings
    for col in ["home_team", "away_team", "stadium", "league", "season", "referee"]:
        if col in m.columns:
            m[col] = m[col].astype(str).str.strip()

    for col in ["team", "player_id", "player_name", "position", "card", "match_id"]:
        if col in s.columns:
            s[col] = s[col].astype(str).str.strip()

    # Asegura match_id string
    if "match_id" in m.columns:
        m["match_id"] = m["match_id"].astype(str).str.strip()
    if "match_id" in s.columns:
        s["match_id"] = s["match_id"].astype(str).str.strip()

    return m, s


# ----------------------------
# Data Quality
# ----------------------------
def data_quality(matches: pd.DataFrame, stats: pd.DataFrame) -> None:
    required_matches = {"match_id", "date", "home_team", "away_team"}
    required_stats = {"match_id", "team", "player_id", "player_name", "minutes"}

    missing_m = required_matches - set(matches.columns)
    missing_s = required_stats - set(stats.columns)

    if missing_m:
        raise ValueError(f"Faltan columnas en matches: {sorted(missing_m)}")
    if missing_s:
        raise ValueError(f"Faltan columnas en player_stats: {sorted(missing_s)}")

    if matches.empty:
        raise ValueError("matches está vacío")
    if stats.empty:
        raise ValueError("player_stats está vacío")

    # match_id no nulo y único en matches
    if matches["match_id"].isna().any():
        raise ValueError("Hay match_id nulos en matches")
    if matches["match_id"].duplicated().any():
        dups = matches.loc[matches["match_id"].duplicated(), "match_id"].head(5).tolist()
        raise ValueError(f"Hay match_id duplicados en matches. Ej: {dups}")

    # integridad referencial: stats.match_id debe existir en matches.match_id
    missing_fk = set(stats["match_id"].dropna()) - set(matches["match_id"])
    if missing_fk:
        raise ValueError(f"Hay match_id en stats que no existen en matches. Ej: {list(missing_fk)[:5]}")

    # rangos razonables (no bloquea si hay NaN)
    if "home_possession_pct" in matches.columns:
        bad = matches.loc[(matches["home_possession_pct"] < 0) | (matches["home_possession_pct"] > 100)]
        if not bad.empty:
            raise ValueError("home_possession_pct fuera de rango 0-100")
    if "away_possession_pct" in matches.columns:
        bad = matches.loc[(matches["away_possession_pct"] < 0) | (matches["away_possession_pct"] > 100)]
        if not bad.empty:
            raise ValueError("away_possession_pct fuera de rango 0-100")

    if "minutes" in stats.columns:
        bad = stats.loc[(stats["minutes"] < 0) | (stats["minutes"] > 130)]
        if not bad.empty:
            raise ValueError("minutes fuera de rango 0-130")

    if matches["date"].isna().any():
        raise ValueError("Hay fechas no parseables en matches (date -> NaT)")


# ----------------------------
# Load Staging (Bronze->Silver->Staging)
# ----------------------------
def load_staging(matches: pd.DataFrame, stats: pd.DataFrame) -> None:
    GOLD.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    con.register("matches_df", matches)
    con.register("stats_df", stats)

    # staging idempotente
    con.execute("CREATE OR REPLACE TABLE stg_matches AS SELECT * FROM matches_df")
    con.execute("CREATE OR REPLACE TABLE stg_player_stats AS SELECT * FROM stats_df")

    con.close()


# ----------------------------
# Build Data Warehouse (Gold)
# ----------------------------
def build_dw() -> None:
    con = duckdb.connect(str(DB_PATH))

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

    # DIM: player (player_id es natural key)
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

    # FACT: player_match (stats a nivel jugador y partido)
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


# ----------------------------
# Gold KPIs (tabla resumen)
# ----------------------------
def build_kpis() -> None:
    con = duckdb.connect(str(DB_PATH))

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

def build_business_views() -> None:
    con = duckdb.connect(str(DB_PATH))

    # 1) Top jugadores por rating (mín. 3 partidos, mín. 60 min media)
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
        HAVING COUNT(*) >= 3 AND AVG(minutes) >= 60
        ORDER BY avg_rating DESC, matches_played DESC
    """)

    # 2) Equipos: goles vs xG (diferencia = “over/under performance”)
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
        ORDER BY goals_minus_xg DESC, matches DESC
    """)

    # 3) Tabla liga: puntos estimados (W=3, D=1, L=0)
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

    # 4) ¿Qué predice mejor ganar? (diferenciales por partido)
    con.execute("""
        CREATE OR REPLACE VIEW vw_win_drivers_deltas AS
        SELECT
            match_id,
            result,
            (home_xG - away_xG) AS delta_xg,
            (home_shots - away_shots) AS delta_shots,
            (home_possession_pct - away_possession_pct) AS delta_possession
        FROM fact_match
        WHERE
            home_xG IS NOT NULL AND away_xG IS NOT NULL
            AND home_shots IS NOT NULL AND away_shots IS NOT NULL
            AND home_possession_pct IS NOT NULL AND away_possession_pct IS NOT NULL
            AND result IS NOT NULL
    """)

    # 5) Top equipos por presión defensiva (tackles+interceptions por 90)
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


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    setup_logging()
    logging.info("START pipeline")

    matches_raw = extract_matches(MATCHES_CSV)
    stats_raw = extract_player_stats(STATS_CSV)

    matches, stats = transform(matches_raw, stats_raw)
    data_quality(matches, stats)

    load_staging(matches, stats)
    build_dw()
    build_kpis()
    build_business_views()

    logging.info("DONE pipeline. DuckDB at %s", DB_PATH)


if __name__ == "__main__":
    main()