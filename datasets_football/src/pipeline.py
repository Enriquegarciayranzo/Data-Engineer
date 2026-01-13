from __future__ import annotations
import logging
from pathlib import Path
from logging_config import setup_logging
from extract import extract_matches, extract_player_stats
from transform import transform
from data_quality import data_quality
from load import load_staging
from dw import build_dw
from kpis import build_kpis
from views import build_business_views


# Paths (keep centralized here)
BASE_DIR = Path(__file__).resolve().parents[1]
BRONZE = BASE_DIR / "data" / "bronze"
GOLD = BASE_DIR / "data" / "gold"
DB_PATH = GOLD / "football_dw.duckdb"

MATCHES_CSV = BRONZE / "futbol_matches.csv"
STATS_CSV = BRONZE / "futbol_player_stats.csv"


def main() -> None:
    setup_logging(BASE_DIR)
    logging.info("START pipeline")

    # Extract raw datasets
    matches_raw = extract_matches(MATCHES_CSV)
    stats_raw = extract_player_stats(STATS_CSV)

    # Transform and validate data
    matches, stats = transform(matches_raw, stats_raw)
    data_quality(matches, stats)

    # Load data and build analytical layer
    load_staging(matches, stats, DB_PATH)
    build_dw(DB_PATH)
    build_kpis(DB_PATH)
    build_business_views(DB_PATH)

    logging.info("DONE pipeline. DuckDB at %s", DB_PATH)


if __name__ == "__main__":
    main()