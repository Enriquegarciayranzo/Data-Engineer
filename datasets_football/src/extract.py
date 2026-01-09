from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_matches(path: Path) -> pd.DataFrame:
    """
    Extract matches dataset.
    Tries latin1 first (common in football datasets), falls back to utf-8.
    """
    try:
        return pd.read_csv(path, encoding="latin1")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="utf-8")


def extract_player_stats(path: Path) -> pd.DataFrame:
    """
    Extract player statistics dataset.
    """
    return pd.read_csv(path, encoding="utf-8")