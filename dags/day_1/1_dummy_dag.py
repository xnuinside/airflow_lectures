from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


with DAG(dag_id="1_dummy_consume_new_data_from_pos",
         start_date=datetime(2020, 12, 1),
         schedule_interval=None) as dag:
    
    get_new_data = DummyOperator(task_id="get_new_data")
    parse_file = DummyOperator(task_id="parse_file")
    
    get_new_data >> parse_file

