import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import settings
import io
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility
import json

log_file_path = '/var/log/datascience-framework-etl.log'

schema = 'operations'
script = os.path.basename(__file__)[:-3]

script_monitor_query =  '''
                            select 
                                * 
                            from 
                                operations.scheduler 
                            where 
                                status != 'success'
                            or 
                                last_update  < last_run - frequency
                            or 
                                last_run < now() - interval '6 hours'
                            and 
                                schema != %(schema)s 
                            and 
                                script != %(script)s;
                        '''

while True:
    schedule_info = None
    scheduler_connection = postgreshandler.get_dashboard_connection()
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
        next_run = utility.get_next_run(start_date, last_run, frequency)

    if now < next_run:
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    postgres_etl_connection = postgreshandler.get_dashboard_connection()
    try:
        with postgres_etl_connection.cursor() as cursor:
            cursor.execute(script_monitor_query,cursor_factory=psycopg2.extras.DictCursor)
            for record in cursor:
                with open(log_file_path, 'a+') as fp:
                    json.dump(record, fp)
                last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

            status = 'success'
            run_time = last_update - start_time

    except Exception as e:
        status = str(e)
    finally:
        postgres_etl_connection.close()

    scheduler_connection = postgreshandler.get_dashboard_connection()
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()