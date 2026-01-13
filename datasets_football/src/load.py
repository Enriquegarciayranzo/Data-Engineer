from __future__ import annotations
from pathlib import Path
import pandas as pd
import duckdb

def load_staging(matches: pd.DataFrame, stats: pd.DataFrame, db_path: Path) -> None:
    # Ensure target directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB and register in-memory DataFrames
    con = duckdb.connect(str(db_path))
    con.register("matches_df", matches)
    con.register("stats_df", stats)

    # Create or replace staging tables
    con.execute("CREATE OR REPLACE TABLE stg_matches AS SELECT * FROM matches_df")
    con.execute("CREATE OR REPLACE TABLE stg_player_stats AS SELECT * FROM stats_df")

    con.close()