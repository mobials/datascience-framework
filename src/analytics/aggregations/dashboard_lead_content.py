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

insert_query =  '''
                    INSERT INTO 
                        {0}.{1}
                    SELECT 
                        id, 
                        master_business_id,
                        integration_settings_id,
                        created_at,
                        device,
                        lead_content
                    FROM
                        autoverify.v_mpm_lead_details
                    WHERE 
                        created_at >= COALESCE(
                                                    (
                                                        SELECT 
                                                            max(created_at) 
                                                        FROM
                                                            autoverify.dashboard_lead_content
                                                    ),
                                                    '2000-01-01'
                                                ) - interval '3 days'
                    AND 
                        created_at < date_trunc('day',
                                                    (
                                                        SELECT 
                                                            MAX(created_at) 
                                                        FROM 
                                                            autoverify.mpm_leads
                                                    )
                                                )
                    AND
                        id 
                    NOT IN 
                    (
                        'db2413bf-b48c-4c39-8a35-f6107abd1a20',
                        'e0476694-a9c8-41ff-b21a-2c8ddc832c47',
                        '4afce89c-3657-43ee-9df1-062359e0dbb3',
                        '15b5f47a-5497-4d95-bc03-854dd05807df',
                        '0be60657-c2ea-4e5f-b921-8fc39c34e00f',
                        '459f1049-1b68-4b02-9093-a89e5e047de9',
                        'ad734d0b-1124-45bc-83e0-f3f336f4f28f',
                        '6781ef80-f282-4b12-85fb-158eb055750a',
                        'df0d8785-e33b-4daf-8797-31548b115c66',
                        'bb05e58f-d792-45af-87bb-8f67396400f7',
                        '82b37b96-4f10-43f5-b645-f337872895e2',
                        'd8ca84f0-de24-4c12-bc67-d61ccd0ee124',
                        '3417ebf0-d9a9-4bfb-87f8-8ee00bb94b21',
                        '501f02ff-6fbc-4f20-b956-151f82491b81',
                        '318f8cbf-fd3e-4baf-bea0-4c841f071ec3',
                        '8169f2db-0b9e-468e-ba98-18ce539e086e',
                        'ad4ba273-26e8-4842-9b58-c83f4e1d4348',
                        '9cf27e54-4d39-4dba-b91e-e130350aa7a9',
                        'fb9bc399-0efd-47a7-87c5-f5fc2587db4e',
                        'c715e1f4-c4a3-48f2-abf2-ac4a3f635ffc',
                        '59440ff3-51fa-4f7d-9b5c-8a04258a6622',
                        '7b6281dc-c44a-4113-990f-8054e51f644a',
                        '030e3078-cd4a-49e3-8754-5a3620737491',
                        'bdf03e8a-81ee-4ee8-8836-852079b43035',
                        '98ac2984-d8c0-45b0-b24e-92bbb7cee297',
                        '1b9219c3-4e1a-4660-acbb-c5c65ccff935',
                        'c08406f0-c1f8-48d3-adf5-9472ef29cacd',
                        '85b778b3-914d-4ad3-903a-b93655829ca5',
                        'b65a4bfd-c299-4211-899b-460a5b7b0f32',
                        'f44fac48-7a90-445a-9aa7-25970475252c',
                        'd7f17625-e03c-4d61-a046-b1eb806c6980',
                        '91b177d0-e743-4475-9ea9-be62e0351a31',
                        'a5a6732c-9221-42e5-aa25-a8e392697c6b',
                        '73501bfa-896a-4011-a5eb-b4a6109c8ee5',
                        '0e8e4a9c-8bc5-4d49-976a-95ac9f455b12'
                    )
                    AND 
                    (
                            sub_type IS NULL
                        OR 
                            sub_type = 'generic'
                    )
                    ORDER BY 
                        created_at ASC
                    ON CONFLICT (id)
                    DO UPDATE 
                    SET 
                        master_business_id = excluded.master_business_id,
                        integration_settings_id = excluded.integration_settings_id,
                        created_at = excluded.created_at,
                        device = excluded.device,
                        lead_content = excluded.lead_content
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
        with postgres_etl_connection.cursor() as cursor:
            cursor.execute(insert_query)
            postgres_etl_connection.commit()
        status = 'success'
        last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        run_time = last_update - start_time

    except Exception as e:
        status = str(e)
    finally:
        postgres_etl_connection.close()

    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection, schema, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()