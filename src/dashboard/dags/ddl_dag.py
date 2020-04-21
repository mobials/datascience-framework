import sys
sys.path.insert(0,'../..')
import airflow
import datetime

start_date = datetime.datetime(2000,1,1)

default_args = {
    'start_date' : start_date,
    'retries': 0
}

dag = airflow.DAG('boot', default_args=default_args,schedule_interval=None)

task = airflow.operators.BashOperator(
    task_id='1',
    bash_command='python /src/dasboard/maintenance/ddl.py',
    dag=dag)

task
