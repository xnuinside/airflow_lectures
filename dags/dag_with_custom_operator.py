from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from custom_operator import HelloOperator


with DAG(dag_id="custom_operator",
         start_date=datetime(2020, 12, 1),
         schedule_interval=None) as dag:

    hello = HelloOperator(task_id="hello")
    get_new_data = DummyOperator(task_id="get_new_data")
    parse_file = DummyOperator(task_id="parse_file")
    
    hello >> get_new_data >> parse_file

