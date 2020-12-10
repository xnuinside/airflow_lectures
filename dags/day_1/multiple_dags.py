from datetime import datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


def create_dag(dag_number,
               params):

    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(str(dag_number)))

    dag = DAG(**params)

    with dag:
        t1 = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_py,
            args=[dag_number])

    return dag


# build a dag for each number in range(10)
for n in range(5):
    dag_id = 'hello_world_{}'.format(str(n))
    params = {'dag_id': dag_id, 
              'schedule_interval': None,
              'start_date': datetime(2020, 12, 1)}
    dag_number = n
    globals()[dag_id] = create_dag(dag_number, params)