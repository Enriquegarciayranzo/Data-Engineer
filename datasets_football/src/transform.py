from __future__ import annotations

import pandas as pd


def transform(
    matches: pd.DataFrame,
    stats: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean and normalize raw datasets.
    """
    m = matches.copy()
    s = stats.copy()

    # Normalize column names
    m.columns = [c.strip() for c in m.columns]
    s.columns = [c.strip() for c in s.columns]

    # Parse date column
    if "date" in m.columns:
        m["date"] = pd.to_datetime(m["date"], errors="coerce")

    # Numeric columns (only if present)
    numeric_cols_matches = [
        "home_goals", "away_goals", "home_shots", "away_shots",
        "home_xG", "away_xG", "home_possession_pct",
        "away_possession_pct", "attendance",
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

    # Clean string columns
    for col in ["home_team", "away_team", "stadium", "league", "season", "referee"]:
        if col in m.columns:
            m[col] = m[col].astype(str).str.strip()

    for col in ["team", "player_id", "player_name", "position", "card", "match_id"]:
        if col in s.columns:
            s[col] = s[col].astype(str).str.strip()

    # Ensure match_id as string
    if "match_id" in m.columns:
        m["match_id"] = m["match_id"].astype(str).str.strip()
    if "match_id" in s.columns:
        s["match_id"] = s["match_id"].astype(str).str.strip()

    return m, s