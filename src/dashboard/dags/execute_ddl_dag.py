import sys
sys.path.insert(0,'../..')
import airflow
import datetime

start_date = datetime.datetime(2000,1,1)

default_args = {
    'start_date' : start_date,
    'retries': 0
}

dag = airflow.DAG('execute_ddl', default_args=default_args,schedule_interval=None)

task = airflow.operators.BashOperator(
    task_id='1',
    bash_command='python3 /var/www/datascience-framework/src/dashboard/maintenance/execute_ddl.py',
    dag=dag)

task
