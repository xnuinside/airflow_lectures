from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


with DAG(
    dag_id="schedule_daily_dag",
    start_date=datetime(2020, 12, 1),
    schedule_interval="0 0 * * *"
    ) as dag:
    get_new_data = DummyOperator(task_id="get_new_data")
    parse_file = DummyOperator(task_id="parse_file")
    check_is_it_ne_customer = DummyOperator(task_id="check_is_it_ne_customer")
    create_new_customer = DummyOperator(task_id="create_new_customer")
    update_existed_customer = DummyOperator(task_id="update_existed_customer")
    
    get_new_data >> parse_file >> check_is_it_ne_customer >> [create_new_customer, update_existed_customer]