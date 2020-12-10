from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor


with DAG(
    dag_id="file_sensor_consume_new_data", 
    start_date=datetime(2020, 12, 1),
    schedule_interval="0 * * * *"
    ) as dag:
    
    # task 1
    get_new_data = FileSensor(task_id="get_new_data", 
                              filepath="../shop123/{{ ds_nodash }}/{{ ts_nodash.split('T')[1][:2] }}/data.json")
    
    # task 2
    parse_file = DummyOperator(task_id="parse_file")
    
    # task 3
    check_is_it_ne_customer = DummyOperator(task_id="check_is_it_ne_customer")
    
    # task 4
    create_new_customer = DummyOperator(task_id="create_new_customer")
    
    # task 5
    update_existed_customer = DummyOperator(task_id="update_existed_customer")
    
    # task 6
    get_new_data >> parse_file >> check_is_it_ne_customer >> [create_new_customer, update_existed_customer]