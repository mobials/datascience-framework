import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import requests
import settings
import time
import csv
import postgreshandler
import datetime
import utility
import pytz
import psycopg2
import psycopg2.extras
import zuorahandler
import os

schema = 'zuora'
script = os.path.basename(__file__)[:-3]

tables = [
    'InvoiceItem',
    'Invoice',
    'CreditMemo',
    'Account',
    'Subscription',
    'Order',
    'Contact',
]

while True:
    schedule_info = None
    scheduler_connection = postgreshandler.get_analytics_connection()
    schedule_info = postgreshandler.get_script_schedule(scheduler_connection,schema,script)
    if schedule_info is None:
        raise Exception('Schedule not found.')
    scheduler_connection.close()

    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    last_run = schedule_info['last_run']
    start_date = schedule_info['start_date']
    frequency = schedule_info['frequency']
    status = schedule_info['status']
    last_update = schedule_info['last_update']
    run_time = schedule_info['run_time']
    next_run = None
    if last_run is None:
        next_run = start_date
    else:
        next_run = utility.get_next_run(start_date,last_run,frequency)

    if now < next_run:
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    etl_connection = postgreshandler.get_analytics_connection()
    try:
        queries = zuorahandler.create_queries(etl_connection,tables)
        bearer_token = zuorahandler.create_bearer_token(settings.zuora_client_id, settings.zuora_client_secret)
        headers = zuorahandler.create_headers(bearer_token)
        batches = zuorahandler.execute_queries(queries, headers)
        updates = 0
        for batch in batches:
            records = batch['recordCount']
            updates += records
            if records > 0:
                data = zuorahandler.get_file_data(batch['fileId'],headers)
                postgreshandler.update_zuora_table(etl_connection,batch['name'],data)

        if updates > 0:
            status = 'success'
            last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
            run_time = last_update - start_time
            etl_connection.commit()
    except Exception as e:
        status = str(e)
    finally:
        etl_connection.close()

    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()
