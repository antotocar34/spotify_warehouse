import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
import sqlite3
from datetime import datetime,timedelta
from airflow.decorators import dag, task 
import os


default_args = {
    'owner'         : 'dj',
    'retries'       : 5,
    'retry_delay'   : timedelta(minutes=2)
}

@dag(dag_id = 'DWBI_finalproject_dag',
    default_args = default_args,
    description = 'This is our DWBI final project ETL DAG.',
    start_date= datetime(2021, 12, 10, 2), 
    schedule_interval='@daily')

def final_etl():

    @task()
    def extract():
        # Establishing a connection to the database
        cnx = sqlite3.connect('playlist.db')
        print("Successfully made contact with the database.")
        
        # Defining the query of interest
        select = 'SELECT * FROM car'

        # Executing the query
        query = cnx.execute(select)

        # Taking the output from the query and putting into Pandas dataframe for processing
        cols = [column[0] for column in query.description]
        results= pd.DataFrame.from_records(data = query.fetchall(), columns = cols)
        print('Successfully placed data in dataframe.')

        # Closing our database connection
        cnx.close()

        # Saving results to a csv file that can then be read by another function
        identifier = str(datetime.date(datetime.now()))
        cwd = os.getcwd()
        path = cwd + "/" + identifier
        results.to_csv(path)
        print('Saved file for future use to {}'.format(path))
        return identifier

    id = extract()
    
etl = final_etl()