from datetime import datetime,timedelta
from airflow.decorators import dag, task 

default_args = {
    'owner'         : 'dj',
    'retries'       : 5,
    'retry_delay'   : timedelta(minutes=2)
}

@dag(dag_id = 'dag_with_taskflow_api_v01',
    default_args = default_args,
    description = 'This is our first dag using airflow dag taskflow api',
    start_date= datetime(2021, 11, 18, 2), 
    schedule_interval='@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name' : 'Jerry',
            'last_name'  : 'Scissorhands'
            }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(first_name,last_name,age):
        print(f"Hello world, my name is {first_name} {last_name}"
              f" and I am {age} years old")

    dicttionary = get_name()
    age = get_age()
    greet(first_name=dicttionary['first_name'],last_name = dicttionary['last_name'],age=age)

greet_dag = hello_world_etl()