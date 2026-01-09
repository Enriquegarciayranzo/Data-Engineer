from __future__ import annotations

from pathlib import Path
import pandas as pd
import duckdb


def load_staging(matches: pd.DataFrame, stats: pd.DataFrame, db_path: Path) -> None:
    """
    Load cleaned datasets into DuckDB staging tables (idempotent).
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.register("matches_df", matches)
    con.register("stats_df", stats)

    con.execute("CREATE OR REPLACE TABLE stg_matches AS SELECT * FROM matches_df")
    con.execute("CREATE OR REPLACE TABLE stg_player_stats AS SELECT * FROM stats_df")

    con.close()