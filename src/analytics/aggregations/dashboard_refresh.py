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

materialized_views = [
    'm_estimated_revenue_quarterly',
    'm_estimated_revenue_yearly',
    'm_estimated_vehicles_sold_monthly',
    'm_lifetime_estimated_revenue',
    'm_lifetime_estimated_vehicles_sold',
    'm_lifetime_master_leads',
    'm_lifetime_master_leads_device',
    'm_lifetime_mixed_leads',
    'm_lifetime_mixed_leads_device',
    'm_master_leads_daily',
    'm_master_leads_daily_device',
    'm_master_leads_monthly',
    'm_master_leads_monthly_device',
    'm_master_leads_weekly',
    'm_master_leads_weekly_device',
    'm_master_leads_yearly',
    'm_master_leads_yearly_device',
    'm_mixed_leads_monthly',
    'm_mixed_leads_monthly_device',
    'm_mixed_leads_yearly',
    'm_mixed_leads_yearly_device'
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
        next_run = utility.get_next_run(start_date, last_run, frequency)

    if now < next_run:
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    postgres_etl_connection = postgreshandler.get_analytics_connection()
    try:
        with postgres_etl_connection.cursor() as cursor:
            for materialized_view in materialized_views:
                query = '''refresh materialized view concurrently {0}.{1}'''.format(schema,materialized_view)
                with postgres_etl_connection.cursor() as cursor:
                    cursor.execute(query)
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