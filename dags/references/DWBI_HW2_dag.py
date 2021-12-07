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

@dag(dag_id = 'DWBI_HW2_dag',
    default_args = default_args,
    description = 'HW2 ETL...',
    start_date= datetime(2021, 11, 20, 2), 
    schedule_interval='@daily')

def dwbi_etl():
    
    @task()
    def extract():
        # Establishing a connection to the database
        cnx = sqlite3.connect('car_db.db')
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


    @task()
    def filter_rows(identifier):
        # Fetching data based on identifier
        cwd = os.getcwd()
        path = cwd + "/" + identifier
        data = pd.read_csv(path)
        print('Successfully fetched the data')

        # Filtering based on our interest.
        # Suppose we are only interested in looking that are NOT BMW model.
        data = data[data['Make'] != 'BMW']
        print(data.head())

        # Dumping the resource at the end of our task.
        data.to_csv(path)
        return identifier


    @task()
    def fetch_origin():
        origin = '{"Porsche" : "Germany","Audi" : "Germany","Toyota" : "Japan"}'
        origin_dict = json.loads(origin)
        return origin_dict

    @task()
    def transform(identifier,origin_dict):
        # Fetching data based on identifier
        cwd = os.getcwd()
        path = cwd + "/" + identifier
        data = pd.read_csv(path)
        print('Successfully fetched the data')

        # Mapping our data based on our interest
        # Suppose that we want to add a country origin column, for analysis later on.
        # But, our city origin data is in the form of a json... so we need to transform..
        data['origin'] = data['Make'].map(origin_dict)

        # Next, we want to group by origin, and we are also interested in columns FuelType,
        # so that we get the following data structure: Country | CarCount | Gasoline.
        df = pd.get_dummies(data, prefix = '',prefix_sep ='', columns=['FuelType'], drop_first = True)
        df = df.groupby('origin').agg({'Gasoline':'count','Make': 'count'}) 
        print(df.head())

        # Finally, suppose we are only interested in the data for Germany, so we will only consider their data:
        df = df[df.index == 'Germany']
        df['origin'] = df.index.astype('str')
        df.reset_index(drop=True,inplace=True)


        # Now, since we are at a different stage in the pipeline, we don't want to name the temp file with the
        # same identifier as before. Therefore, we will come up with a new identifier
        identifier_2 = 'final_output'
        path = cwd + "/" + identifier_2

        # Finally, saving the temp file.
        df.to_csv(path)
        return identifier_2

    @task()
    def load(identifier_2):
        # Fetching data based on identifier_2
        cwd = os.getcwd()
        path = cwd + "/" + identifier_2
        data = pd.read_csv(path)
        print('Successfully fetched the data')
        print(data.head())

        # Opening database connection
        cnx = sqlite3.connect('car_db.db')
        cursor=cnx.cursor()
        print("Successfully made contact with the database.")
        

        # Loading our data into a separate table in our database(warehouse)
        for i, row in data.iterrows():
            cursor.execute("INSERT INTO CountryStats (Country,CarCount,GasolineCount) values(?,?,?)", (row.origin, row.Make, row.Gasoline))

        # Closing connection
        cnx.close()


    id = extract()
    id = filter_rows(id)
    id_2 = transform(id,fetch_origin())
    load(id_2)

etl_dag = dwbi_etl()
