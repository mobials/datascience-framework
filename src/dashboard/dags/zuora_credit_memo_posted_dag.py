import sys
sys.path.insert(0,'../..')
import airflow
import datetime

start_date = datetime.datetime.now() - datetime.timedelta(minutes=60)

default_args = {
    'start_date' : start_date,
    'retries': 0
}

dag = airflow.DAG('zuora_credit_memo_posted', default_args=default_args,schedule_interval='0 * * * *', max_active_runs=1)

task = airflow.operators.BashOperator(
    task_id='1',
    bash_command='python3 /var/www/datascience-framework/src/dashboard/etl/zuora_credit_memo_posted.py',
    dag=dag)