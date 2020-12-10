import os
import json
from random import choice
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

# dag config {"shop_id":"shop256"}

def shop_filepath_macros(shop_id, date, hour):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{shop_id}/{date}/{hour}/data.json")
    return file_path


def parse_json(file_path, **context):
    for key, item in context.items():
        print(key, item)
    with open(file_path, 'r') as data_file:
        print(file_path)
        content = json.load(data_file)
        print(content)
        context['ti'].xcom_push(key='data', value=content)
        return True

file_path = "{{ shop_filepath_macros(dag_run.conf.get('shop_id', 'shop123'), ds_nodash, ts_nodash.split('T')[1][:2])}}"


def branch_callable(**context):
    client_in_db = choice([True, False])
    if client_in_db:
        return 'update_existed_customer'
    return 'create_new_customer'


with DAG(
    dag_id="max_active_dag_run_one_with_xcom_branch_operator", 
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
                              filepath=file_path)
    
    # task 2
    parse_file = PythonOperator(task_id="parse_file", 
                                python_callable=parse_json, 
                                provide_context=True, 
                                op_kwargs={'file_path': file_path})
    
    # task 3
    check_is_it_ne_customer = BranchPythonOperator(
        task_id="check_is_it_ne_customer", 
        python_callable=branch_callable)
    
    # task 4
    create_new_customer = DummyOperator(task_id="create_new_customer")
    
    # task 5
    update_existed_customer = DummyOperator(task_id="update_existed_customer")
    
    # task 6
    get_new_data >> parse_file >> check_is_it_ne_customer >> [create_new_customer, update_existed_customer]