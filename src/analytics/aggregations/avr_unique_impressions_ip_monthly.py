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

schema = 'public'
script = os.path.basename(__file__)[:-3]

insert_query = '''
                INSERT INTO {0}.{1}
                SELECT 
                    master_business_id,
                    DATE_TRUNC('month',happened_at) AS date,
                    COUNT(distinct ip_address) AS impressions
                FROM 
                (
                    SELECT 
                        (payload->>'master_business_id')::uuid AS master_business_id,
                        happened_at,
                        (payload->>'ip_address') AS ip_address
                    FROM 
                        s3.avr_widget_impressions
                    WHERE 
                        happened_at >= %(start_date)s
                    AND 
                        happened_at < %(end_date)s
                    UNION
                    SELECT 
                        (payload->>'masterBusinessId')::uuid AS master_business_id,
                        happened_at,
                        (payload->>'ipAddress') AS ip_address
                    FROM 
                        s3.integrations_widget_was_rendered
                    WHERE 
                        happened_at >= %(start_date)s
                    AND 
                        happened_at < %(end_date)s
                    UNION
                    SELECT 
                        (payload->>'masterBusinessId')::uuid AS master_business_id,
                        happened_at,
                        (payload->>'ipAddress') AS ip_address
                    FROM 
                        s3.integrations_button_widget_was_rendered
                    WHERE 
                        happened_at >= %(start_date)s
                    AND 
                        happened_at < %(end_date)s
                ) a                 
                GROUP BY
                    1,2
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

        min_date_integrations_widget_was_rendered = postgreshandler.get_min_value(postgres_etl_connection,'s3','integrations_widget_was_rendered','happened_at')
        min_date_integrations_button_widget_was_rendered = postgreshandler.get_min_value(postgres_etl_connection, 's3','integrations_button_widget_was_rendered','happened_at')
        min_date_avr_widget_impressions = postgreshandler.get_min_value(postgres_etl_connection, 's3','avr_widget_impressions','happened_at')

        max_date_integrations_widget_was_rendered = postgreshandler.get_max_value(postgres_etl_connection,'s3','integrations_widget_was_rendered','happened_at')
        max_date_integrations_button_widget_was_rendered = postgreshandler.get_max_value(postgres_etl_connection, 's3','integrations_button_widget_was_rendered','happened_at')
        max_date_avr_widget_impressions = postgreshandler.get_max_value(postgres_etl_connection, 's3','avr_widget_impressions','happened_at')

        max_date_avr_widget_impressions_ip_monthly = postgreshandler.get_max_value(postgres_etl_connection, schema,script,'date')

        if min_date_integrations_widget_was_rendered is None:
            raise Exception('min_date_integrations_widget_was_rendered is None')
        if min_date_avr_widget_impressions is None:
            raise Exception('min_date_avr_widget_impressions is None')
        if min_date_integrations_button_widget_was_rendered is None:
            raise Exception('min_date_integrations_button_widget_was_rendered is None')

        first_date = utility.add_months(max_date_avr_widget_impressions_ip_monthly,1) if max_date_avr_widget_impressions_ip_monthly is not None else utility.get_month(min(min_date_integrations_widget_was_rendered,min_date_avr_widget_impressions,min_date_integrations_button_widget_was_rendered))
        last_date = utility.get_month(min(max_date_integrations_widget_was_rendered,max_date_avr_widget_impressions,max_date_integrations_button_widget_was_rendered))

        if first_date < last_date:
            for date in utility.get_months_from(first_date,last_date):
                print(date)
                start_date = date
                end_date = utility.add_months(start_date,1)

                with postgres_etl_connection.cursor() as cursor:
                    cursor.execute(insert_query,{'start_date':start_date,'end_date':end_date})
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