from pathlib import Path
import hashlib

BASE = Path("./files")
DIR_OK = BASE / "validations/ok"


def file_hash(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


def main():
    print("\nDUPLICATES: Searching duplicates")

    hashes = {}
    duplicates = []

    for file_path in DIR_OK.rglob("*.txt"):
        h = file_hash(file_path)

        if h in hashes:
            duplicates.append((file_path, hashes[h]))
        else:
            hashes[h] = file_path

    if duplicates:
        print("\nFound duplicate files:")
        for dup, original in duplicates:
            print(f" - {dup.name}  (same as {original.name})")
    else:
        print("No duplicates found. All files are unique!")
    print("Duplicates module finished.\n")

if __name__ == "__main__":
    main()