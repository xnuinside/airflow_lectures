import os
from random import choice
import json
from time import sleep
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor


# dag config {"shop_id":"shop256"}

def parse_json(file_path, **context):
    print('#### Context start')
    for key, item in context.items():
        print(key, item)
    print('#### Context end')
    with open(file_path, 'r') as data_file:
        print(file_path)
        content = json.load(data_file)
        print(content)
        # context['ti'].xcom_push(key='data', value=content)
        return content


def shop_filepath_macros(shop_id, date, hour):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, f"{shop_id}/{date}/{hour}/data.json")
    return file_path


file_path = "{{ shop_filepath_macros(dag_run.conf['shop_id'] or 'shop123', ds_nodash, ts_nodash.split('T')[1][:2])}}"


def branch_callable():
    exists = choice([True, False])
    print(exists, 'exists')
    if exists:
        return 'update_existed_customer'
    return 'create_new_customer'


def failed_callable_task(task_id, **context):
    sleep(60)
    return True


def failure_callback(context):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir,'failed.txt')
    
    with open(file_path, 'w+') as file_:
        file_.write(context['ti'].task_id)


default_args = {'on_failure_callback': failure_callback, 'execution_timeout': timedelta(seconds=1)}


with DAG(
    dag_id="file_sensor_consume_new_data_max_active_run_1", 
    start_date=datetime(2020, 12, 1),
    #schedule_interval="0 * * * *", 
    schedule_interval=None,
    max_active_runs=1,
    user_defined_macros={
        'shop_filepath_macros': shop_filepath_macros
    },
    default_args=default_args
    ) as dag:
    
    # NEW COMMENT!
    
    # task 1
    # {{ ts_nodash.split('T')[1][:2] }}
    # shop123
    get_new_data = FileSensor(task_id="get_new_data", 
                              filepath=file_path)
    
    # task 2
    parse_file = PythonOperator(
        task_id="parse_file", 
        python_callable=parse_json,
        provide_context=True,
        op_kwargs={'file_path': file_path}
        )
    
    
    # task 3
    check_is_it_ne_customer = BranchPythonOperator(
        task_id="check_is_it_ne_customer",
        python_callable=branch_callable)
    
    # task 4
    create_new_customer = PythonOperator(
        task_id="create_new_customer", 
        python_callable=failed_callable_task,
        provide_context=True,
        op_args=["create_new_customer"])
    
    # task 5
    update_existed_customer = PythonOperator(
        task_id="update_existed_customer", 
        python_callable=failed_callable_task,
        provide_context=True,
        op_args=["update_existed_customer"])
    
    # task 5
    final_status = DummyOperator(
        task_id="final_status",
        trigger_rule='none_failed'
        )
    
    # task 6
    get_new_data >> parse_file >> check_is_it_ne_customer >> [create_new_customer, update_existed_customer] >> final_status