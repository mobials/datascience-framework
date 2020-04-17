from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
'retries' : 1,
'start_date' : datetime(2020,4,17)
}



dag =  DAG('zuora_invoice_item_created_dag', default_args=default_args,schedule_interval=None)

t1 = BashOperator(dag=dag, task_id='running_script', bash_command='python ../etl/zuora_invoice_item_created.py' )

t1