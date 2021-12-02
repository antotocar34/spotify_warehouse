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
        cnx = sqlite3.connect("playlist.db")
        print("Successfully made contact with the database.")

        # Defining the query of interest
        tables = ["songs", "playlists", "playlistssongs"]
        selects = [
            f"SELECT * FROM {table}"
            for table in tables
        ]

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
        OUT_PATH = Path(f"{os.getenv('AIRFLOW_HOME')}/data/") 
        assert OUT_PATH.exists(), "Data output path is not found"
        for df, table_name in zip(dfs, tables):
            with open(OUT_PATH / f"{table_name}.pkl", 'wb') as f:
                dump(df, f)

        print(f"Saved file for future use to {str(OUT_PATH)}")
        return tables

    table_ids = extract_db()

etl_out = etl()
