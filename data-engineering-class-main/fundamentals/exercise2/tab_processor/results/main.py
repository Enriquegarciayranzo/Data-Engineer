import os
from pathlib import Path

BASE = Path("./files")

DIR_SONGS = BASE / "songs"
DIR_CLEANED = BASE / "cleaned"
DIR_OK = BASE / "validations/ok"
DIR_KO = BASE / "validations/ko"


def count_files(path: Path):
    if not path.exists():
        return 0
    return sum(1 for f in path.rglob("*") if f.is_file())


def main():
    print("RESULTS")

    songs = count_files(DIR_SONGS)
    cleaned = count_files(DIR_CLEANED)
    ok = count_files(DIR_OK)
    ko = count_files(DIR_KO)

    print(f"Songs downloaded: {songs}")
    print(f"Songs cleaned: {cleaned}")
    print(f"Valid OK: {ok}")
    print(f"Valid KO: {ko}")

if __name__ == "__main__":
    main()