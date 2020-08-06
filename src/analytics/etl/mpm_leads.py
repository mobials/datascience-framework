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
reupload_window = int(-1440*3)

#minutes lag
lag_window = 0

read_query = '''
                SELECT 
	                mpm_leads.*,
	                mpm_lead_details.*
                FROM 
	                mpm_leads
                LEFT JOIN 
                    mpm_lead_details
                ON 
                    mpm_lead_details.lead_id = mpm_leads.id
                WHERE 
	                mpm_leads.status != 'created'
	            AND 
	                mpm_leads.updated_at >= %(min_updated_at)s 
	            AND
	                mpm_leads.updated_at < %(max_updated_at)s
	            ORDER BY 
	                mpm_leads.updated_at ASC
            '''

insert_query = '''
                    INSERT INTO
                        {0}.{1}
                    (
                        id,
                        created_at,
                        updated_at,
                        payload
                    )
                    VALUES 
                        %s
                    ON CONFLICT (id)
                    DO UPDATE 
                    SET 
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        payload = EXCLUDED.payload
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
        next_run = utility.get_next_run(start_date, last_run, frequency)

    if now < next_run:
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    postgres_etl_connection = postgreshandler.get_analytics_connection()
    try:
        last_updated_at = postgreshandler.get_max_value(postgres_etl_connection,schema,script,'updated_at')
        min_updated_at = last_updated_at if last_updated_at is not None else datetime.datetime(2000,1,1).replace(tzinfo=pytz.utc)
        #add reupload time
        min_updated_at = utility.add_minutes(min_updated_at,reupload_window)
        max_updated_at = utility.add_minutes(datetime.datetime.utcnow(),lag_window)
        tuples = []
        mysql_etl_connection = mysqlhandler.get_autoverify_connection()
        try:
            with mysql_etl_connection.cursor() as cursor:
                cursor.execute(read_query, {'min_updated_at': min_updated_at, 'max_updated_at': max_updated_at})
                columns = [col[0] for col in cursor.description]
                #count = 0
                for row in cursor:
                    #count += 1
                    #print(count)
                    info = dict(zip(columns, row))
                    id = info['id']
                    created_at = info['created_at']
                    updated_at = info['updated_at']

                    # clean up some fields we don't need in the payload
                    del info['id']
                    del info['created_at']
                    del info['updated_at']
                    del info['lead_id']

                    # convert the remaining entries into json
                    payload = json.dumps(info, default=str)

                    tuple = (
                        id,
                        created_at,
                        updated_at,
                        payload,
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