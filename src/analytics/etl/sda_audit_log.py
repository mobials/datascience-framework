import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import settings
import io
import zipfile
import json
import postgreshandler
import mysqlhandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility


schema = 'autoverify'
script = os.path.basename(__file__)[:-3]

#minutes to rescan
reupload_window = 0

#minutes lag
lag_window = 15

read_query = '''
                SELECT 
	                *
                FROM 
	                {0}
                WHERE 
	                happened_at >= %(min_happened_at)s 
	            AND
	                happened_at < %(max_happened_at)s
	            ORDER BY 
	                happened_at ASC
            '''.format(script)

insert_query = '''
                    INSERT INTO
                        {0}.{1}
                    (
                        id,
                        user_id,
                        happened_at,
                        request_method,
                        request_payload,
                        response_code,
                        response_payload,
                        endpoint
                    )
                    VALUES 
                        %s
                    ON CONFLICT (id)
                    DO UPDATE 
                    SET 
                        user_id = EXCLUDED.user_id,
                        happened_at = EXCLUDED.happened_at,
                        request_method = EXCLUDED.request_method,
                        request_payload = EXCLUDED.request_payload,
                        response_code = EXCLUDED.response_code,
                        response_payload = EXCLUDED.response_payload,
                        endpoint = EXCLUDED.endpoint
                '''.format(schema,script)
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

    postgres_etl_connection = postgreshandler.get_analytics_connection()
    try:
        last_happened_at = postgreshandler.get_max_value(postgres_etl_connection,schema,script,'happened_at')
        min_happened_at = last_happened_at if last_happened_at is not None else datetime.datetime(2000,1,1).replace(tzinfo=pytz.utc)
        #add reupload time
        min_happened_at = utility.add_minutes(min_happened_at,reupload_window)
        max_happened_at = utility.add_minutes(datetime.datetime.utcnow(),lag_window)

        tuples = []
        mysql_etl_connection = mysqlhandler.get_autoverify_connection()
        try:
            with mysql_etl_connection.cursor(buffered=True) as cursor:
                cursor.execute(read_query,{'min_happened_at':min_happened_at,'max_happened_at':max_happened_at})
                columns = [col[0] for col in cursor.description]
                for row in cursor:
                    info = dict(zip(columns, row))
                    id = info['id']
                    happened_at = info['happened_at']
                    user_id = info['user_id']
                    request_method = info['request_method']
                    #request_payload = json.dumps(info['request_payload'],default=str)
                    request_payload = psycopg2.extras.Json(info['request_payload'])
                    response_code = int(info['response_code']) if info['response_code'] is not None else None

                    #response_payload = json.dumps(info['response_payload'],default=str)
                    response_payload = psycopg2.extras.Json(info['response_payload'])
                    endpoint = info['endpoint']

                    tuple = (
                        id,
                        user_id,
                        happened_at,
                        request_method,
                        request_payload,
                        response_code,
                        response_payload,
                        endpoint,
                    )
                    tuples.append(
                        tuple
                    )
        finally:
            mysql_etl_connection.close()

        if len(tuples) > 0:
            with postgres_etl_connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor,insert_query,tuples)
                postgres_etl_connection.commit()
                status = 'success'
                last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                run_time = last_update - start_time
    except Exception as e:
        status = str(e)
    finally:
        postgres_etl_connection.close()

    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()