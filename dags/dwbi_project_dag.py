import os
import requests
import json
from typing import List
from datetime import datetime, timedelta
from uuid import uuid4
from pickle import dump
from pathlib import Path

from airflow.decorators import dag, task
import sqlite3
from sqlite3 import Error
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd

default_args = {
    "owner": "dj",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
assert AIRFLOW_HOME, "AIRFLOW_HOME environemnt variable is not set."
DATA_HOME = Path(f"{AIRFLOW_HOME}/data")
assert DATA_HOME.exists()
FACT_DIMENSION_DB_PATH = f"{DATA_HOME}/factdb/data_warehouse.db"


@dag(
    dag_id="DWBI_finalproject_dag",
    default_args=default_args,
    description="This is our DWBI final project ETL DAG.",
    start_date=datetime(2021, 12, 1, 2),
    schedule_interval="@daily",
)
def etl():
    @task()
    def extract_db() -> List[str]:
        """
        This task extracts from the database tables
        """
        # Establishing a connection to the database
        cnx = sqlite3.connect(f"{AIRFLOW_HOME}/playlist.db")
        print("Successfully made contact with the database.")

        # Defining the query of interest
        tables = ["songs", "playlists", "playlistssongs"]
        # Select everythin from the database
        selects = {table: f"SELECT * FROM {table}" for table in tables}

        # Executing the query
        queries = {
            name: cnx.execute(select) for name, select in selects.items()
        }

        def df_from_query(query):
            """
            Takes a query and stores it into a pandas dataframe.
            """
            cols = [column[0] for column in query.description]
            result_df = pd.DataFrame.from_records(
                data=query.fetchall(), columns=cols
            )
            print("Successfully placed data in dataframe.")
            return result_df

        dfs = {name: df_from_query(query) for name, query in queries.items()}

        # Closing our database connection
        cnx.close()

        # Saving results to a csv file that can then be read by another function
        OUT_PATH = Path(DATA_HOME / "interrim")
        assert OUT_PATH.exists(), "Data output path is not found"
        for table_name, df in dfs.items():
            with open(OUT_PATH / f"{table_name}.pkl", "wb") as f:
                dump(df, f)

        print(f"Saved file for future use to {str(OUT_PATH)}")
        return [f"{table}.pkl" for table in tables]

    @task()
    def extract_api_data() -> List[str]:
        """
        Extract api data from csv.
        Store as dataframe and
        return a list of Paths.
        """
        assert os.getenv("AIRFLOW_HOME")
        api_datas = [
            "audio_features.csv",
            "audio_features_top.csv",
            "toptracks.csv",
        ]
        paths = [(DATA_HOME / Path(s)) for s in api_datas]
        assert all(map(lambda p: p.exists(), paths)), print(paths)
        for p in paths:
            df = pd.read_csv(p)
            df.to_pickle(p.parent.resolve() / f"api_{p.name}")
        return [str(p.parent / f"api_{p.name}") for p in paths]

    @task()
    def transform_data(table_pickles: List[str], api_data_csvs: List[str]):
        """
        Create fact and dimension tables.
        """
        # Define audio feature names
        audio_features = [
            "danceability",
            "energy",
            "key",
            "loudness",
            "mode",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
            "duration_ms",
        ]
        ## load data
        table_dfs = [
            pd.read_pickle(f"{DATA_HOME}/interrim/{table}")
            for table in table_pickles
        ]
        api_data_dfs = [pd.read_pickle(path) for path in api_data_csvs]

        # Unpack the dataframes
        songs_table_df = table_dfs[0]
        playlists_table_df = table_dfs[1]
        pssongs_df = table_dfs[2]
        audio_features_df = api_data_dfs[0]
        audio_top_tracks_df = api_data_dfs[1]
        top_tracks_df = api_data_dfs[2].drop_duplicates(subset="track_uri")

        # Make the playlist fact table
        playlist_fact = (
            pssongs_df.merge(
                audio_features_df,
                left_on="track_uri",
                right_on="song_uri",
                how="left",
            )
            .drop(
                ["song_uri", "Unnamed: 0", "track_href", "analysis_url", "id"],
                axis=1,
            )
            .merge(playlists_table_df, how="left", on="playlist_id")
            .merge(
                songs_table_df[["artist_uri", "track_uri"]],
                on="track_uri",
                how="left",
            )
            .drop("playlist_name", axis=1)
        )

        # Make the dimension tables
        dimension_songs = (
            audio_features_df[["song_uri", "duration_ms"]]
            .merge(
                songs_table_df[["track_uri", "track_name"]],
                how="right",
                left_on="song_uri",
                right_on="track_uri",
            )
            .drop("song_uri", axis=1)
        )

        dimension_playlists = playlists_table_df.drop("num_followers", axis=1)

        dimension_artists = songs_table_df[
            ["artist_uri", "artist_name"]
        ].drop_duplicates()

        # Making top track fact table
        top_track_fact = (
            top_tracks_df[["track_uri", "artist", "popularity"]]
            .merge(
                audio_top_tracks_df[audio_features + ["track_uri"]],
                how="left",
                on="track_uri",
            )
            .rename(
                columns={
                    "artist": "artist_uri",
                    "spotify": "spotify_popularity",
                }
            )
        )

        dimension_top_songs = top_tracks_df[
            ["track_uri", "track_name", "explicit", "duration_ms"]
        ]
        dimension_top_artists = (
            top_tracks_df[["artist", "artist_name"]]
            .rename({"artist": "artist_uri"}, axis=1)
            .drop_duplicates(subset="artist_uri")
        )

        facts: List[pd.DataFrame] = [playlist_fact, top_track_fact]

        dimensions: List[pd.DataFrame] = [
            dimension_playlists,
            dimension_artists,
            dimension_songs,
            dimension_top_songs,
            dimension_top_artists,
        ]

        pkl_fact_names = list(
            map(lambda s: f"fact_{s}", ["playlist", "top_track"])
        )
        pkl_dimension_names = list(
            map(
                lambda s: f"dimension_{s}",
                ["playlists", "artists", "songs", "top_songs", "top_artists"],
            )
        )
        for d, name in zip(
            dimensions + facts, pkl_dimension_names + pkl_fact_names
        ):
            d.to_csv(f"{DATA_HOME}/interrim/{name}.csv")

        return pkl_dimension_names + pkl_fact_names

    @task
    def load(csv_names: List[str]):
        """
        Load data into a databse that is based on the star schema.
        """

        dfs = [
            pd.read_csv(f"{DATA_HOME}/interrim/{csv_name}.csv", index_col=0)
            for csv_name in csv_names
        ]

        (
            dimension_playlists,
            dimension_artists,
            dimension_songs,
            dimension_top_songs,
            dimension_top_artists,
        ) = dfs[0:5]
        fact_playlist, fact_top_track = dfs[5:7]

        table_name_mapping = {
            "dimension_playlists": dimension_playlists,
            "dimension_artists": dimension_artists,
            "dimension_songs": dimension_songs,
            "dimension_top_songs": dimension_top_songs,
            "dimension_top_artists": dimension_top_artists,
            "fact_playlist": fact_playlist,
            "fact_top_track": fact_top_track,
        }

        def create_connection(db_file):
            """create a database connection to a SQLite database"""
            conn = None
            try:
                conn = sqlite3.connect(db_file)
                with open(f"{AIRFLOW_HOME}/db2_creation.txt", "r") as f:
                    create_statement = f.read()
                conn.executescript(create_statement)
                print(sqlite3.version)
            except Error as e:
                print(f"ERROR: {e}")
            return conn

        data_base_path = f"{AIRFLOW_HOME}/data/factdb/data_warehouse.db"
        conn = create_connection(data_base_path)
        cursor = conn.cursor()

        def insert_df_into_database(cursor, conn, table: str):
            out = cursor.execute(f"SELECT * FROM {table} limit1")
            # Get attributes/columns of a table.
            column_names = [row[0] for row in out.description]
            df = table_name_mapping[table]
            # assert that columns of dataframe are the same as the attributes of the table
            assert list(df.columns) == column_names, set(
                df.columns
            ).symmetric_difference(set(column_names))

            tuples = [tuple(row) for _, row in df.iterrows()]

            sql = f"INSERT OR IGNORE INTO {table} values ({', '.join(['?' for _ in column_names])})"
            print(f"Inserted into {table}")
            print(sql, tuples[0])
            cursor.executemany(sql, tuples)
            conn.commit()
            return

        for table in table_name_mapping:
            insert_df_into_database(cursor, conn, table)

        conn.close()

        return list(table_name_mapping.keys())
    
    @task()
    def shift_to_Qlik(table_names: List[str]):

        conn = sqlite3.connect(FACT_DIMENSION_DB_PATH)

        for table in table_names:
            table_df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            table_df.to_csv(f"./data/qlik/{table}.csv")

        return

    table_pickles = extract_db()
    api_data_csvs = extract_api_data()
    csv_names = transform_data(table_pickles, api_data_csvs)
    table_names = load(csv_names)
    out = shift_to_Qlik(table_names)
    


etl_out = etl()
