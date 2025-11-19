# Importamos las bibliotecas necesarias
import os
import re
import logging as log
import datetime
from pathlib import Path
from utils.string_mapping import MAPPING

# -- Configuration ---
INPUT_DIRECTORY = Path("./files/")
CATALOG_DIRECTORY = INPUT_DIRECTORY / "catalogs"
LOGS_DIRECTORY = Path("./logs/")
OUTPUT_DIRECTORY = INPUT_DIRECTORY / "cleaned/"

MIN_LINES = 5

dir_list = []

# --- Logging config---
log.basicConfig(
    filename=LOGS_DIRECTORY / "cleaner.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)

logger = log.getLogger(__name__)

# --- Logic---

def list_files_recursive(path: Path):
    """
    Recorre todos los ficheros dentro de `path`, ignorando:
      - La carpeta `catalogs`
      - Cualquier fichero que no sea .txt
    """
    for entry in path.iterdir():
        if entry.is_dir():
            if entry.name == "catalogs":
                continue
            list_files_recursive(entry)
        else:
            if entry.suffix.lower() != ".txt":
                continue
            dir_list.append(entry)

    return dir_list


def remove_email_sentences(text: str):
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    sentence_pattern = r"[\n^.!?]*" + email_pattern + r"[^.!?]*[.!?\n]"
    return re.sub(sentence_pattern, "", text)


def apply_format_rules(text: str):
    formatted_text = remove_email_sentences(text)

    for key, value in MAPPING.items():
        formatted_text = re.sub(
            key,
            value,
            formatted_text,
            flags=re.DOTALL | re.IGNORECASE
        )

    return formatted_text


def main():

    # Start time tracking
    start_time = datetime.datetime.now()
    log.info(f"Cleaner started at {start_time}")
    print("Starting cleaner...")

    cleaned = 0

    for file_path in list_files_recursive(INPUT_DIRECTORY):
        log.info(f"Processing file: {file_path}")

        text = file_path.read_text(encoding="utf-8", errors="ignore")

        if text.count("\n") < MIN_LINES:
            log.info("Empty or too small tab. Skipping.")
            continue

        # Apply cleaning
        formatted_text = apply_format_rules(text)

        # Calculate secure output path
        relative = file_path.relative_to(INPUT_DIRECTORY)
        output_path = OUTPUT_DIRECTORY / relative

        # Create directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save cleaned file
        output_path.write_text(formatted_text, encoding="utf-8")

        cleaned += 1
        print(cleaned, "--", output_path.name, "CREATED!!")

    end_time = datetime.datetime.now()
    log.info(f"Cleaner ended at {end_time}")
    duration = end_time - start_time
    log.info(f"Total duration: {duration}")

    print(
        f"Cleaner finished. Duration in seconds: {duration.total_seconds()}, "
        f"that is {duration.total_seconds() / 60} minutes."
    )


if __name__ == "__main__":
    main()
