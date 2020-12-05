from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor


with DAG(
    dag_id="file_sensor_consume_new_data", 
    start_date=datetime(2020, 12, 1),
    schedule_interval="0 * * * *"
    ) as dag:
    get_new_data = FileSensor(task_id="get_new_data", 
                              filepath="../shop123/{{ ds_nodash }}/${hour}/data.json")
    parse_file = DummyOperator(task_id="parse_file")
    check_is_it_ne_customer = DummyOperator(task_id="check_is_it_ne_customer")
    create_new_customer = DummyOperator(task_id="create_new_customer")
    update_existed_customer = DummyOperator(task_id="update_existed_customer")
    
    get_new_data >> parse_file >> check_is_it_ne_customer >> [create_new_customer, update_existed_customer]