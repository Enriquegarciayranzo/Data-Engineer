import os
import click
import re
import logging as log
import datetime
import shutil
from pathlib import Path

# --- Directories ---
FILES = Path("files")
CLEANED = FILES / "cleaned"
VALID_OK = FILES / "validations" / "ok"
VALID_KO = FILES / "validations" / "ko"


# --- Validation rules ---
def validate_song(text: str) -> bool:
    """Flexible validation: at least 2 lines OR contains chords."""
    if text.count("\n") >= 2:
        return True

    chord_pattern = r"\b([A-G](#|b)?m?(aj)?7?)\b"
    if re.search(chord_pattern, text, flags=re.IGNORECASE):
        return True

    return False


# --- File listing ---
def get_cleaned_files():
    """Yield all txt files under cleaned/, recursively."""
    return CLEANED.rglob("*.txt")


@click.command()
@click.option("--init", "-i", is_flag=True, default=False)
def main(init):

    print("Starting validator...")

    if init:
        shutil.rmtree(VALID_OK, ignore_errors=True)
        shutil.rmtree(VALID_KO, ignore_errors=True)
        print("Previous validations removed.")

    ok = ko = 0

    for path in get_cleaned_files():

        text = path.read_text(encoding="utf-8", errors="ignore")

        # Compute relative path
        rel = path.relative_to(CLEANED)

        # Remove "songs/" prefix if it exists
        parts = rel.parts
        if parts[0].lower() == "songs":
            rel = Path(*parts[1:])

        # Compute destination
        if validate_song(text):
            dest = VALID_OK / rel
            ok += 1
        else:
            dest = VALID_KO / rel
            ko += 1

        # Ensure parent folder exists
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Save file
        dest.write_text(text, encoding="utf-8")

        print(f"OK={ok}  KO={ko}  → {rel}")

    print("\nValidator finished.")
    print(f"TOTAL OK = {ok}")
    print(f"TOTAL KO = {ko}")


if __name__ == "__main__":
    main()