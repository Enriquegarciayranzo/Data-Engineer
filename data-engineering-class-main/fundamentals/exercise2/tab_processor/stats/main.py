from pathlib import Path

BASE = Path("./files")
DIR_OK = BASE / "validations/ok"

def count_stats(text):
    lines = text.count("\n") + 1
    words = len(text.split())
    chars = len(text)
    return lines, words, chars


def main():
    print("\nSTATS: ")

    total_files = 0
    total_words = 0
    total_chars = 0

    for file_path in DIR_OK.rglob("*.txt"):
        text = file_path.read_text(encoding="utf-8", errors="ignore")

        lines, words, chars = count_stats(text)
        total_files += 1
        total_words += words
        total_chars += chars

        print(f"{file_path.name}: {lines} lines | {words} words | {chars} chars")

    print("\nSUMMARY")
    print(f"Total files analyzed: {total_files}")
    print(f"Total words: {total_words}")
    print(f"Total characters: {total_chars}")
    print("STATS module finished.\n")
    
if __name__ == "__main__":
    main()