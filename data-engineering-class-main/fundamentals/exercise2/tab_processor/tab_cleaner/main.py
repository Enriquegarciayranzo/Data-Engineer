import os
import re
import logging as log
import datetime
from pathlib import Path
from utils.string_mapping import MAPPING

# --- Configuration ---
INPUT_DIRECTORY = Path("./files/")
OUTPUT_DIRECTORY = INPUT_DIRECTORY / "cleaned/"
LOGS_DIRECTORY = Path("./logs/")
MIN_LINES = 5

dir_list = []


# --- Logging config ---
log.basicConfig(
    filename=LOGS_DIRECTORY / "cleaner.log",
    filemode="w",
    encoding="utf-8",
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=log.INFO,
)


# --- File discovery ---
def list_files_recursive(path: Path):
    for entry in path.iterdir():
        if entry.is_dir():

            # No entrar en catalogs ni en cleaned
            if entry.name in ("catalogs", "cleaned"):
                continue

            list_files_recursive(entry)

        else:
            if entry.suffix.lower() != ".txt":
                continue
            dir_list.append(entry)

    return dir_list


# --- Cleaning helpers ---
def remove_email_sentences(text: str):
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    sentence_pattern = r"[\n^.!?]*" + email_pattern + r"[^.!?]*[.!?\n]"
    return re.sub(sentence_pattern, "", text)


def apply_format_rules(text: str):
    formatted = remove_email_sentences(text)
    for key, value in MAPPING.items():
        formatted = re.sub(key, value, formatted, flags=re.DOTALL | re.IGNORECASE)
    return formatted


# --- Main cleaner ---
def main():
    start_time = datetime.datetime.now()
    print("Starting cleaner...")

    cleaned = 0
    files_to_process = list_files_recursive(INPUT_DIRECTORY)

    for file_path in files_to_process:
        if not file_path.exists():
            print("MISSING →", file_path)
            continue

        text = file_path.read_text(encoding="utf-8", errors="ignore")

        if text.count("\n") < MIN_LINES:
            continue

        formatted = apply_format_rules(text)

        # build normalized relative path
        relative = file_path.relative_to(INPUT_DIRECTORY)
        relative = Path(str(relative).lower())

        # --- FIX: evitar rutas duplicadas cleaned/cleaned ---
        if str(relative).startswith("cleaned/"):
            relative = Path(str(relative)[8:])
        # ----------------------------------------------------

        output_path = OUTPUT_DIRECTORY / relative

        # create the path if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # save cleaned file
        output_path.write_text(formatted, encoding="utf-8")

        cleaned += 1
        print(f"{cleaned} -- {output_path.name} CREATED!!")

    end = datetime.datetime.now()
    duration = end - start_time
    print(f"Cleaner finished in {duration.total_seconds()} seconds.")


if __name__ == "__main__":
    main()