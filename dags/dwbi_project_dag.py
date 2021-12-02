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

        merged_df = songs_table_df.merge(
            audio_features_df,
            right_on="song_uri",
            left_on="track_uri",
            how="left",
        ).drop("Unnamed: 0", axis=1)

        pssongs_df = table_dfs[2].copy()
        pssongs_df["playlist_popularity"] = 1
        pssongs_df.drop(columns=["pos", "playlist_id"], axis=1, inplace=True)
        track_playlist_popularity_df = (
            pssongs_df.groupby("track_uri")
            .sum()
            .sort_values(by="playlist_popularity", ascending=False)
            .reset_index()
        )
        dimension_songs = merged_df.merge(
            track_playlist_popularity_df, on="track_uri", how="left"
        ).drop(
            ["artist_name", "artist_uri", "album_uri", "song_uri", "id"],
            axis=1,
        )

        playlistsongs_df = table_dfs[2]
        list_of_features = [
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
        mean_feature_per_playlist = (
            playlistsongs_df.merge(dimension_songs, on="track_uri", how="left")
            .groupby("playlist_id")
            .agg({f: "mean" for f in list_of_features})
        ).reset_index()

        dimension_playlists = table_dfs[1].merge(mean_feature_per_playlist, on="playlist_id", how="left") \
                                          .drop("collaborative", axis=1)

        top_songs_df = api_data_dfs[2]
        top_audio_features_df = api_data_dfs[1]
        dimension_top_songs = ...

        dimension_artists = ...

        return ["hi"]

    table_ids = extract_db()
    api_data_csvs = extract_api_data()
    out = transform_data(table_ids, api_data_csvs)


etl_out = etl()
