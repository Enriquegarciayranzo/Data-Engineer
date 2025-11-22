from pathlib import Path
import re

BASE = Path("./files")
DIR_OK = BASE / "validations/ok"

CHORD_PATTERN = r"\b([A-G][#b]?m?(maj7)?(sus2|sus4|add9)?(/[A-G][#b]?)?)\b"
BRACKET_PATTERN = r"\[[A-G][#b]?m?(maj7)?(sus2|sus4|add9)?(/[A-G][#b]?)?\]"


def remove_chords(text: str) -> str:
    """Elimina acordes entre [] y acordes sueltos."""
    text = re.sub(BRACKET_PATTERN, "", text)
    text = re.sub(CHORD_PATTERN, "", text)

    clean_lines = []
    for line in text.splitlines():
        if re.fullmatch(r"\s*(" + CHORD_PATTERN + r"\s*)+", line):
            continue
        clean_lines.append(line)
    return "\n".join(clean_lines)

def process_ok_files():
    print("=== LYRICS MODULE: Removing chords ===")

    for file_path in DIR_OK.rglob("*.txt"):

        if not file_path.exists():
            continue

        try:
            text = file_path.read_text(encoding="utf-8")
            lyrics_only = remove_chords(text)

            new_path = file_path.with_name(file_path.stem + "_lyrics.txt")
            new_path.write_text(lyrics_only, encoding="utf-8")

            print("Processed:", new_path.name)

        except Exception as e:
            print(f"Skipped {file_path.name}: {e}")
            continue

    print("Lyrics module finished.")

def main():
    process_ok_files()

if __name__ == "__main__":
    main()