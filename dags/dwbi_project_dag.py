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
        # Establishing a connection to the database
        cnx = sqlite3.connect(f"{AIRFLOW_HOME}/playlist.db")
        print("Successfully made contact with the database.")

        # Defining the query of interest
        tables = ["songs", "playlists", "playlistssongs"]
        selects = [f"SELECT * FROM {table}" for table in tables]

        # Executing the query
        queries = [cnx.execute(select) for select in selects]

        # Taking the output from the query and putting into Pandas dataframe for processing
        def df_from_query(query):
            cols = [column[0] for column in query.description]
            result_df = pd.DataFrame.from_records(
                data=query.fetchall(), columns=cols
            )
            print("Successfully placed data in dataframe.")
            return result_df

        dfs = [df_from_query(query) for query in queries]

        # Closing our database connection
        cnx.close()

        # Saving results to a csv file that can then be read by another function
        OUT_PATH = Path(DATA_HOME / "interrim")
        assert OUT_PATH.exists(), "Data output path is not found"
        for df, table_name in zip(dfs, tables):
            with open(OUT_PATH / f"{table_name}.pkl", "wb") as f:
                dump(df, f)

        print(f"Saved file for future use to {str(OUT_PATH)}")
        return [f"{table}.pkl" for table in tables]

    @task()
    def extract_api_data() -> List[str]:
        """ """
        # TODO Parse json
        assert os.getenv("AIRFLOW_HOME")
        api_datas = [
            "audio_features.csv",
            "audio_features_top.csv",
            "toptracks.csv",
        ]
        paths = [(DATA_HOME / Path(s)).exists() for s in api_datas]
        assert all(paths), print(paths)
        return api_datas

    @task()
    def transform_data(table_pickles: List[str], api_data_csvs: List[str]):
        """
        Create fact and dimension tables.
        """
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
        api_data_dfs = [
            pd.read_csv(f"{DATA_HOME}/{csv}") for csv in api_data_csvs
        ]

        songs_table_df = table_dfs[0]
        audio_features_df = api_data_dfs[0]
        playlists_table_df = table_dfs[1]
        pssongs_df = table_dfs[2]

        playlist_analysis = pssongs_df.merge(audio_features_df, left_on="track_uri", right_on="song_uri", how="left") \
                                      .drop(["song_uri", "Unnamed: 0", "track_href", "analysis_url", "id"], axis=1) \
                                      .merge(playlists_table_df, how="left", on="playlist_id") \
                                      .merge(songs_table_df[["artist_uri", "track_uri"]], on="track_uri", how="left") \
                                      .drop("playlist_name", axis=1)

        playlist_analysis["song_popularity_per_playlist"] = playlist_analysis["track_uri"].map(dict(pssongs_df.track_uri.value_counts()))


        dimension_songs = audio_features_df[["song_uri", "duration_ms"]].merge(
                songs_table_df[["track_uri", "track_name"]], how="right", left_on="song_uri", right_on="track_uri"
                ).drop("song_uri", axis=1)


        dimension_playlists = playlists_table_df.drop("num_followers", axis=1)

        dimension_artists = songs_table_df[["artist_uri", "artist_name"]].drop_duplicates()

        audio_top_tracks_df = api_data_dfs[1]
        top_tracks_df = api_data_dfs[2].drop_duplicates(subset="track_uri")

        top_track_analysis = top_tracks_df[["track_uri", "artist", "popularity"]].merge(
                audio_top_tracks_df[audio_features + ["track_uri"]], how="left", on="track_uri"
                ).rename(
                        columns={"artist": "artist_uri",
                                 "spotify": "spotify_popularity"}
                        )
                                                                                

        dimension_top_tracks = top_tracks_df[["track_uri", "track_name", "explicit", "duration_ms"]]
        dimension_artists = top_tracks_df[["artist", "artist_name"]].rename({"artist":"artist_uri"}, axis=1)


        facts: List[pd.DataFrame] = [playlist_analysis, top_track_analysis]

        dimensions: List[pd.DataFrame] = [dimension_playlists,
                                          dimension_artists,
                                          dimension_songs,
                                          dimension_top_tracks,
                                          dimension_top_artists]

        
        pkl_fact_names = list(map(lambda s: f"fact_{s}", ["playlists", "artists", "songs", "top_songs"]))
        pkl_dimension_names = list(map(lambda s: f"dimension_{s}", ["playlists", "artists", "songs", "top_songs"]))
        for d, name in zip(dimensions + facts, pkl_dimension_names + pkl_fact_names):
            d.to_csv(f"{DATA_HOME}/interrim/{name}.csv")

        return pkl_dimension_names + pkl_fact_names

    @task
    def load(pkl_dimension_names: List[str]):
        def create_connection(db_file):
            """ create a database connection to a SQLite database """
            conn = None
            try:
                conn = sqlite3.connect(db_file)
                print(sqlite3.version)
            except Error as e:
                print(e)
            finally:
                if conn:
                    conn.close()

    table_pickles = extract_db()
    api_data_csvs = extract_api_data()
    out = transform_data(table_pickles, api_data_csvs)


etl_out = etl()
