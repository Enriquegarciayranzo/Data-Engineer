from __future__ import annotations

import pandas as pd

# Validate data quality.
# Ensures required columns, non-empty datasets, key integrity, and reasonable value ranges.
def data_quality(matches: pd.DataFrame, stats: pd.DataFrame) -> None:
    required_matches = {"match_id", "date", "home_team", "away_team"}
    required_stats = {"match_id", "team", "player_id", "player_name", "minutes"}

    missing_m = required_matches - set(matches.columns)
    missing_s = required_stats - set(stats.columns)

    # Check that all required columns are present in the matches dataset
    if missing_m:
        raise ValueError(f"Missing columns in matches: {sorted(missing_m)}")
    # Check that all required columns are present in the player statistics dataset
    if missing_s:
        raise ValueError(f"Missing columns in player_stats: {sorted(missing_s)}")
    # Ensure the matches dataset is not empty
    if matches.empty:
        raise ValueError("matches is empty")
    # Ensure the player statistics dataset is not empty 
    if stats.empty:
        raise ValueError("player_stats is empty")

    # match_id not null/empty and unique in matches
    if matches["match_id"].isna().any():
        raise ValueError("Null match_id found in matches")
    
    # Ensure match_id is unique to avoid duplicate matches in the dataset
    if matches["match_id"].duplicated().any():
        dups = matches.loc[matches["match_id"].duplicated(), "match_id"].head(5).tolist()
        raise ValueError(f"Duplicated match_id in matches. Example: {dups}")

    # Referential integrity: stats.match_id must exist in matches.match_id
    missing_fk = set(stats["match_id"].dropna()) - set(matches["match_id"])
    if missing_fk:
        example = list(missing_fk)[:5]
        raise ValueError(f"match_id in stats not found in matches. Example: {example}")

    # Reasonable ranges (ignore NaN)
    if "home_possession_pct" in matches.columns:
        bad = matches.loc[
            (matches["home_possession_pct"] < 0) | (matches["home_possession_pct"] > 100)
        ]
        if not bad.empty:
            raise ValueError("home_possession_pct out of range 0-100")

    # Validate that away team possession percentages are within a realistic range (0–100)
    if "away_possession_pct" in matches.columns:
        bad = matches.loc[
            (matches["away_possession_pct"] < 0) | (matches["away_possession_pct"] > 100)
        ]
        if not bad.empty:
            raise ValueError("away_possession_pct out of range 0-100")

    # Validate that player minutes are within a realistic match range
    if "minutes" in stats.columns:
        bad = stats.loc[(stats["minutes"] < 0) | (stats["minutes"] > 130)]
        if not bad.empty:
            raise ValueError("minutes out of range 0-130")

    # Ensure match dates were correctly parsed
    if matches["date"].isna().any():
        raise ValueError("Unparseable dates found in matches (date -> NaT)")