GLOB = 3
PRELIM = """
CREATE TABLE Streams (
      StreamID INTEGER NOT NULL UNIQUE,
      UserID INTEGER NOT NULL,
      SongID INTEGER NOT NULL,
      PlaylistID INTEGER,
      Time TEXT NOT NULL,
      PRIMARY KEY(StreamID),
      FOREIGN KEY(UserID) REFERENCES Users(UserID),
      FOREIGN KEY(SongID) REFERENCES Songs(SongID),
      FOREIGN KEY(PlaylistID) REFERENCES Playlists(PlaylistID)
    );

CREATE TABLE Songs (
        SongID    INTEGER NOT NULL UNIQUE,
        SongName  TEXT NOT NULL,
        Artist    TEXT NOT NULL,
        Album     TEXT,
        Duration  INTEGER,
        ReleaseYear TEXT,
        PRIMARY KEY(SongID)
        );

CREATE TABLE Playlists (
        PlaylistID    INTEGER NOT NULL,
        CreatorID     INTEGER,
        PlaylistName  TEXT,
        PlaylistLink  TEXT,
        PRIMARY KEY(PlaylistID),
        FOREIGN KEY(CreatorID) REFERENCES Users(UserID)
        );

CREATE TABLE PlaylistSongs (
        PlaylistID    INTEGER,
        SongID        INTEGER NOT NULL,
        PRIMARY KEY(PlaylistID, SongID),
        FOREIGN KEY(PlaylistID) REFERENCES Playlists(PlaylistID),
        FOREIGN KEY(SongID) REFERENCES Songs(SongID)
        );

CREATE TABLE StreamingSessions (
        SessionID INTEGER,
        StreamID  INTEGER,
        StartingTime TEXT,
        EndTime TEXT,
        PRIMARY KEY(SessionID, StreamID),
        FOREIGN KEY(StreamID) REFERENCES Streams(StreamID)
        );

CREATE TABLE Users (
        UserID        INTEGER NOT NULL,
        Name          TEXT,
        Address       TEXT,
        PaymentInformation    TEXT,
        PRIMARY KEY(UserID)
        );

CREATE TABLE dim_playlist (
    CreatorID INTEGER,
    PlaylistID INTEGER,
    Playlistname TEXT,
    Playlistlink TEXT
);

CREATE TABLE dim_songs_playlists (
    PlaylistID INTEGER,
    SongID INTEGER,
    SongName TEXT,
    Artist TEXT
);
"""

print(PRELIM)
from dateutil import parser
from typing import Tuple, Any
from datetime import timedelta
import random
from faker import Faker


fake = Faker()
random.seed(123)
Faker.seed(123)

new_line_char = "\n"
end_char = ";"
tab_char = "\t"


def create_insert(table_name: str, data: Tuple[Any]):
    output = f"""
    INSERT INTO {table_name}
    VALUES {data[0]},
           {new_line_char.join([ str(value) + "," for value in data[1:-1]])}
           {data[-1]}{end_char}
    """
    print(output)
    return output + "\n"

class User:
    def __init__(self):
        self.id = fake.unique.iana_id()
        self.name = fake.name()
        self.address = fake.address()
        self.iban = fake.iban()

    def props(self):
        prop = (self.id, self.name, self.address, self.iban)
        return prop

num_users = 50
users = [
    User()
    for _ in range(num_users)
]

insert_user = create_insert("Users", [user.props() for user in users])

num_songs = 300
class Song:
    def __init__(self):
        self.id = fake.unique.iana_id()
        self.name = fake.word().capitalize()
        self.artist = fake.name()
        self.album = ' '.join(fake.words(2)).capitalize()
        self.release = fake.year()
        self.duration = random.randrange(50,500) # seconds

    def props(self):
        data = (
            self.id,
            self.name,
            self.artist,
            self.album,
            self.duration,
            self.release
        )
        return data


songs = [
        Song()
        for _ in range(num_songs)
        ]

insert_song = create_insert("Songs", [song.props() for song in songs])

num_playlists = 20
class Playlist:
    def __init__(self):
        if GLOB != 3:
            breakpoint()
        self.id = fake.unique.iana_id()
        self.name = ' '.join(fake.words(2)).capitalize()
        self.creator_id = random.sample(users, 1)[0].id
        self.url = f"www.spotify.com/{fake.unique.iana_id()}"
        self.songs = random.sample(songs, random.randrange(10,30))

    def props(self):
        data = (
            self.id,
            self.creator_id,
            self.name,
            self.url,
        )
        return data
    

playlists = [
           Playlist()
           for p in range(num_playlists)
           ]

create_insert("Playlists", [playlist.props() for playlist in playlists])

create_insert("PlaylistSongs", [(playlist.id, song.id)  for playlist in playlists for song in playlist.songs])


num_streams = 60
class Stream:
    def __init__(self):
        self.id = fake.unique.iana_id()
        self.user_id = random.sample(users, 1)[0].id
        coin_toss = random.randrange(1,10)
        if coin_toss > 8:
            playlist = random.sample(playlists, 1)[0]
            self.playlist_id = playlist.id
            self.song_id = random.sample(playlist.songs, 1)[0].id
        else:
            self.playlist_id = 'NULL'
            self.song_id = random.sample(songs, 1)[0].id
        self.time = random.randrange(20, 1283)

    def props(self):
        props = (
        self.id, 
        self.user_id,
        self.song_id,
        self.playlist_id,
        self.time
        )
        return props


streams = [Stream() for _ in range(num_streams)]

create_insert("Streams", [stream.props() for stream in streams])


streamingsessions = [
        (
            fake.unique.iana_id(),
            stream.id,
            ( t := fake.iso8601()),
            ( (parser.parse(t) + timedelta(random.randrange(1,3))).isoformat() ),
        )
        for stream in streams
        ]
instert_ss = create_insert("StreamingSessions", streamingsessions)

# os.system(f"rm spot_hw.db && sqlite3 spot_hw.db -cmd {PRELIM}{insert_user}{insert_song}{insert_playlist}{instert_ss}")
