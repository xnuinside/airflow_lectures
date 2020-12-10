import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor

# dag config {"shop_id":"shop256"}

def shop_filepath_macros(shop_id, date, hour):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{shop_id}/{date}/{hour}/data.json")
    return file_path


with DAG(
    dag_id="max_active_dag_run_1", 
    start_date=datetime(2020, 12, 1),
    schedule_interval=None,
    #schedule_interval="0 * * * *",
    user_defined_macros={
        'shop_filepath_macros': shop_filepath_macros
    },
    max_active_runs=1
    ) as dag:
    # task 1
    get_new_data = FileSensor(task_id="get_new_data", 
                              filepath="{{ shop_filepath_macros(dag_run.conf['shop_id'], ds_nodash, ts_nodash.split('T')[1][:2])}}")
    
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