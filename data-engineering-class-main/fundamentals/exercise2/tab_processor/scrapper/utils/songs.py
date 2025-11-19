import logging as log
import sys
from scrapper.utils import beautifulsoup as bs
from scrapper.utils import files
from scrapper.utils.data import Song, Artist
import re
import time
from pathlib import Path

# --- Configuration ---
ROOT = "https://acordes.lacuerda.net"
URL_ARTIST_INDEX = "https://acordes.lacuerda.net/tabs/"
SONG_VERSION = None
INDEX = "abcdefghijklmnopqrstuvwxyz"


# --- Utility Functions ---
def get_version(song, version: int = 0):
    song = (
        str(song)
        if not version
        else str(song).replace(".shtml", f"-{str(version)}.shtml")
    )
    song_name = str(song).split("/")[-1].replace(".shtml", ".txt")
    return song, song_name


def get_artists(start_char: str, end_char: str) -> list[Artist]:
    log.info("Starting to build artists catalog...")
    artists = []
    for char_code in range(ord(start_char), ord(end_char) + 1):
        char = chr(char_code)
        artist_index_url = f"{URL_ARTIST_INDEX}/{char}"
        log.info(f"Scraping artist index: {artist_index_url}")

        soup = bs.get_soup(artist_index_url)
        if not soup:
            continue

        ul_tag = soup.find("ul")
        if not ul_tag:
            log.info(f"No <ul> found on {artist_index_url}", file=sys.stderr)
            continue

        for li in ul_tag.find_all("li"):
            a_tag = li.find("a")
            if a_tag and a_tag.get("href"):
                href = ROOT + a_tag["href"]
                artist_display_name = Path(href).name.replace("_", " ").title()
                artists.append(Artist(name=artist_display_name, url=href))

    return artists


def get_catalog(output_directory: Path, start_char: str = "a", end_char: str = "z") -> dict:
    start_char = start_char.lower()
    end_char = end_char.lower()

    catalog = get_artists(start_char, end_char)

    for artist in catalog:
        log.info(f"Scraping songs for artist: {artist.name} ({artist.url})")
        soup = bs.get_soup(artist.url)
        if not soup:
            continue

        for a_tag in soup.select("li > a"):
            if a_tag and a_tag.get("href") and not a_tag["href"].startswith("http"):
                song_relative_path = a_tag["href"]

                if not artist.url.endswith("/") and not song_relative_path.startswith("/"):
                    song_base_url_prefix = f"{artist.url}/"
                else:
                    song_base_url_prefix = artist.url

                url = f"{song_base_url_prefix}{song_relative_path}.shtml"
                full_song_url, song_filename = get_version(url, SONG_VERSION)

                song_title = Path(song_relative_path).stem.replace("_", " ").title()
                song_output_dir = f"{output_directory}songs/{artist.name.replace(' ', '_').lower()}/{song_filename}"

                artist.songs.append(
                    Song(
                        song_title=song_title,
                        song_url=full_song_url,
                        genre="",
                        lyrics_path=song_output_dir,
                    )
                )

    log.info("Cataloging complete.")
    return catalog


def get_song_lyrics(song_name: str, song_url: str, song_file_path: str) -> str:
    try:
        song_file_path = files.normalize_relative_path(song_file_path)

        if files.check_file_exists(song_file_path):
            log.info(f"File {song_file_path} already exists. Skipping download.")
            return False

        log.info("song --> %s - url --> %s", song_name, song_url)

        try:
            lyric = bs.get_soup(song_url).findAll("pre")
        except Exception as e:
            log.error(f"Error fetching song from {song_url}: {e}")
            return False

        for p in lyric:
            text = re.sub("<.*?>", "", str(p)).strip()
            if text:
                files.write_string_to_file(song_file_path, text=text)
                print(song_name, "downloaded!")
                return True

    except Exception as e:
        log.error(f"Error fetching lyrics from {song_url}: {e}")
        raise e


# ----------------------------------------------------------
# NEW get_songs() — the version required for the exercise
# ----------------------------------------------------------

def get_songs(output_directory: str, version: int = 0):
    """
    Usa catalog.json para descargar las canciones.
    No vuelve a scrapear la web.
    """

    catalog_path = Path(output_directory) / "catalogs" / "catalog.json"
    catalog_data = files.load_from_json(catalog_path)

    if not catalog_data:
        log.error("No catalog found. Run get_catalog() first.")
        return

    for artist in catalog_data:   # aquí tu JSON encaja perfecto
        artist_name = artist["name"]
        log.info(f"Downloading songs for artist: {artist_name}")

        for song in artist["songs"]:
            song_name = song["song_title"]
            song_url = song["song_url"]
            lyrics_path = song["lyrics_path"]

            # aplicar versión si hace falta (0 = original)
            song_url_versioned, _ = get_version(song_url, version)

            try:
                if get_song_lyrics(song_name, song_url_versioned, lyrics_path):
                    time.sleep(0.5)
            except Exception as e:
                log.error(f"Error downloading {song_name}: {e}")
                continue

    log.info("Song download process complete.")
