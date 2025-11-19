import musicbrainzngs
from scrapper.utils import files   # ← CORRECTO
from dataclasses import dataclass, asdict, field
from pathlib import Path

# --- Config ---

# Initialize MusicBrainz client
musicbrainzngs.set_useragent("MyMusicApp", "1.0", "myemail@example.com")


# --- Data Structures ---
@dataclass
class Song:
    """Represents a song with its metadata."""

    id: int = field(init=False)
    song_title: str
    song_url: str
    genre: str = ""
    lyrics_path: Path = None

    _id_counter = 1

    def __post_init__(self):
        self.id = Song._id_counter
        Song._id_counter += 1

        # normalize path safely
        if self.lyrics_path:
            self.lyrics_path = files.normalize_relative_path(self.lyrics_path)

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(data):
        data_copy = data.copy()
        data_copy.pop("id", None)

        if "lyrics_path" in data_copy and data_copy["lyrics_path"]:
            data_copy["lyrics_path"] = Path(data_copy["lyrics_path"])

        song = Song(**data_copy)

        if "id" in data and data["id"] >= Song._id_counter:
            Song._id_counter = data["id"] + 1

        return song

    @classmethod
    def reset_id_counter(cls, start_value=1):
        cls._id_counter = start_value


@dataclass
class Artist:
    """Represents an artist with their name, URL, and songs."""

    id: int = field(init=False)
    name: str
    url: str
    genres: list[str] = field(default_factory=list)
    albums: list[str] = field(default_factory=list)
    songs: list[Song] = field(default_factory=list)

    _id_counter = 1

    def __post_init__(self):
        self.id = Artist._id_counter
        Artist._id_counter += 1

        # Fetch metadata
        self.fetch_metadata()

    def to_dict(self):
        data = asdict(self)
        data["songs"] = [song.to_dict() for song in self.songs]
        return data

    def to_dict_no_songs(self):
        data = asdict(self)
        data.pop("songs", None)
        return data

    def fetch_metadata(self):
        try:
            results = musicbrainzngs.search_artists(artist=self.name, limit=1)
            if results["artist-list"]:
                artist_data = results["artist-list"][0]
                mbid = artist_data["id"]

                details = musicbrainzngs.get_artist_by_id(
                    mbid, includes=["tags", "releases"]
                )

                if "tag-list" in details["artist"]:
                    self.genres = [tag["name"] for tag in details["artist"]["tag-list"]]

                if "release-list" in details:
                    self.albums = list({r["title"] for r in details["release-list"]})

        except Exception as e:
            print(f"Error fetching data for {self.name}: {e}")

    @staticmethod
    def from_dict(data):
        data_copy = data.copy()
        data_copy.pop("id", None)

        songs_data = data_copy.pop("songs", [])
        artist = Artist(**data_copy)
        artist.songs = [Song.from_dict(s) for s in songs_data]

        if "id" in data and data["id"] >= Artist._id_counter:
            Artist._id_counter = data["id"] + 1

        return artist

    @classmethod
    def reset_id_counter(cls, start_value=1):
        cls._id_counter = start_value
