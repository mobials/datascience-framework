#import sys
#sys.path.insert(0,'../..')
import airflow
import datetime
import time

start_date = datetime.datetime.now() - datetime.timedelta(minutes=60)

default_args = {
    'start_date' : start_date,
    'retries': 0
}

dag = airflow.DAG('execute_ddl', default_args=default_args,schedule_interval=None,max_active_runs=1)

task = airflow.operators.BashOperator(
    task_id='1',
    bash_command='python3 /var/www/datascience-framework/src/dashboard/maintenance/execute_ddl.py',
    dag=dag)

task
