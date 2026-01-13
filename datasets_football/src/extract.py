from __future__ import annotations

from pathlib import Path
import pandas as pd

# Read matches CSV file. Handles common encoding issues in football datasets.
def extract_matches(path: Path) -> pd.DataFrame:
    # Try different encodings to ensure robust CSV ingestion.
    try:
        return pd.read_csv(path, encoding="latin1")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")

# Read player statistics CSV file.
def extract_player_stats(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")