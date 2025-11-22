from pathlib import Path
import re
from collections import Counter

BASE = Path("./files")
DIR_OK = BASE / "validations/ok"
INSIGHTS_DIR = BASE / "insights"

STOPWORDS = {
    "el","la","los","las","de","del","y","o","a","que","en","por","con","para",
    "un","una","unos","unas","se","me","te","mi","tu","su","al","lo","es","no",
    "si","ya","muy","mas","pero","como","cuando","donde","porque","soy","eres",
    "somos","son","era","fue","han","ha","he","sus","este","ese","esa","estos",
    "esas","aqui","alli","ahi"
}

def clean_word(word: str) -> str:
    return re.sub(r"[^a-záéíóúüñ]", "", word.lower())

def extract_words(text: str):
    words = []
    for word in text.split():
        w = clean_word(word)
        if len(w) > 2 and w not in STOPWORDS:
            words.append(w)
    return words

def process_artist(artist_name: str, artist_dir: Path):
    full_text = ""
    all_words = []

    for file_path in artist_dir.rglob("*_lyrics.txt"):
        text = file_path.read_text(encoding="utf-8")
        full_text += text + "\n"
        all_words += extract_words(text)

    INSIGHTS_DIR.mkdir(parents=True, exist_ok=True)

    full_lyrics_path = INSIGHTS_DIR / f"{artist_name}_full_lyrics.txt"
    full_lyrics_path.write_text(full_text, encoding="utf-8")

    counter = Counter(all_words)
    top10 = counter.most_common(10)

    top10_path = INSIGHTS_DIR / f"{artist_name}_top10.txt"
    top10_path.write_text(
        "\n".join([f"{w}: {c}" for w, c in top10]),
        encoding="utf-8"
    )

    print(f"[OK] Artista processed → {artist_name}")
    print(f"- Total words: {len(all_words)}")
    print(f"- Top10 generated: {top10_path.name}")

    return all_words

def main():
    print("=== INSIGHTS MODULE ===")

    all_global_words = []

    for artist_dir in DIR_OK.iterdir():
        if not artist_dir.is_dir():
            continue

        artist_name = artist_dir.name

        artist_words = process_artist(artist_name, artist_dir)
        all_global_words += artist_words

    global_top20 = Counter(all_global_words).most_common(20)

    global_path = INSIGHTS_DIR / "global_top20.txt"
    global_path.write_text(
        "\n".join([f"{w}: {c}" for w, c in global_top20]),
        encoding="utf-8"
    )

    print("[DONE] Top 20 created.")
    print("Insights module finished.")

if __name__ == "__main__":
    main()