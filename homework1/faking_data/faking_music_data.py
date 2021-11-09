PRELIM = """
CREATE TABLE "Streams" (
        "StreamID"  INTEGER NOT NULL UNIQUE,
        "UserID"    INTEGER NOT NULL,
        "SongID"    INTEGER,
        "PlaylistID"    INTEGER,
        "Time"  BLOB NOT NULL,
        FOREIGN KEY("UserID") REFERENCES "Users"("UserID")
        FOREIGN KEY("SongID") REFERENCES "Songs"("SongID")
        FOREIGN KEY("PlaylistID") REFERENCES "Playlists"("PlaylistID")
        PRIMARY KEY("StreamID")
        );

CREATE TABLE "Songs" (
        "SongID"    INTEGER NOT NULL UNIQUE,
        "SongName"  TEXT NOT NULL,
        "Artist"    TEXT NOT NULL,
        "Album"     TEXT,
        "Duration"  INTEGER,
        "ReleaseYear" TEXT,
        PRIMARY KEY("SongID")
        );

CREATE TABLE "StreamingSessions" (
        "SessionID" INTEGER,
        "StreamID"  INTEGER,
        "StartingTime" TEXT,
        "End Time" TEXT,
        FOREIGN KEY("StreamID") REFERENCES "Streams"("StreamID"),
        PRIMARY KEY("SessionID", "StreamID")
        );

CREATE TABLE "Playlists" (
        "PlaylistID"    INTEGER NOT NULL,
        "CreatorID"     INTEGER,
        "PlaylistName"  TEXT,
        "PlaylistLink"  TEXT,
        "NumberOfSongs" INTEGER,
        FOREIGN KEY("CreatorID") REFERENCES "Users"("UserID"),
        PRIMARY KEY("PlaylistID")
        );

CREATE TABLE "Users" (
        "UserID"        INTEGER NOT NULL,
        "Name"      TEXT,
        "Address"       TEXT,
        "PaymentInformation"    TEXT,
        PRIMARY KEY("UserID")
        );
"""

print(PRELIM)
import numpy as np
from datetime import datetime, timedelta
from dateutil import parser
import random
from faker import Faker
import os


fake = Faker()
random.seed(123)
Faker.seed(123)

num_users = 500


users = [
    (fake.unique.iana_id(), fake.name(), fake.address(), fake.iban())
    for _ in range(num_users)
]

new_line_char = "\n"
end_char = ";"
tab_char = "\t"


def create_insert(table_name, data):
    output = f"""
    INSERT INTO {table_name}
    VALUES {data[0]},
           {new_line_char.join([ str(value) + "," for value in data[1:-1]])}
           {data[-1]}{end_char}
    """
    print(output)
    return output + "\n"


insert_user = create_insert("Users", users)


creators = list(
    map(
        lambda t: t[3],
        random.sample(users, 10),
    )
)

playlist = [
    (
        fake.unique.iana_id(),
        creator_id,
        f"www.spotify.com/{fake.unique.iana_id()}",
        " ".join(fake.words(2)).capitalize(),
        random.randrange(10, 30),
    )
    for creator_id in creators
]

insert_playlist = create_insert("Playlists", playlist)


num_songs = 2000
songs = [
        (
            fake.unique.iana_id(),
            fake.word().capitalize(),
            fake.name(),
            ' '.join(fake.words(2)).capitalize(),
            random.randrange(1,5),
            fake.year()
        )
        for _ in range(num_songs)
        ]

insert_song = create_insert("Songs", songs)

streams = [(fake.unique.iana_id(), random.sample(users, 1)[0][0], 'NULL', x[0], fake.iso8601()) for x in random.sample(playlist, 5)] + [(fake.unique.iana_id(), random.sample(users, 1)[0][0], x[0], 'NULL', fake.iso8601()) for x in random.sample(songs, 50)]

create_insert("Streams", streams)


streamingsessions = [
        (
            fake.unique.iana_id(),
            stream[0],
            ( t := fake.iso8601()),
            ( (parser.parse(t) + timedelta(random.randrange(1,3))).isoformat() ),
        )
        for stream in streams
        ]
instert_ss = create_insert("StreamingSessions", streamingsessions)

os.system(f"rm spot_hw.db && sqlite3 spot_hw.db -cmd {PRELIM}{insert_user}{insert_song}{insert_playlist}{instert_ss}")


