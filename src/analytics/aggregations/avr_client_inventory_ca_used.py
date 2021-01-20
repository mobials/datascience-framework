import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import settings
import io
import zipfile
import json
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility

schema = 'aggregations'
script = os.path.basename(__file__)[:-3]
days = 7
timelapse_seconds = 86400
insert_query =  '''
                    truncate table {0}.{1};
                    insert into {0}.{1}
                    select distinct on (a.master_business_id,a.dealer_id, b.vin)
                        a.master_business_id,
                        a.dealer_id,
                        b.vin,
                        b.status_date,
                        b.price,
                        b.miles
                    from 
                        aggregations.v_client_marketcheck_ca_used a,
                        vendors.marketcheck_ca_used b 
                    where 
                        b.dealer_id = a.dealer_id
                    and 
                        b.status_date >= now() - interval '{2} days'
                    order by 
                        1,2,3,4 desc;
                '''.format(schema,script,days)

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

            max_marketcheck_status_date = postgreshandler.get_max_value(postgres_etl_connection,'vendors','marketcheck_ca_used','status_date')
            max_inventory_status_date = postgreshandler.get_max_value(postgres_etl_connection,schema,script,'status_date')

            if max_inventory_status_date is None:
                max_inventory_status_date = datetime.datetime(2000,1,1).replace(tzinfo=pytz.utc)

            if max_marketcheck_status_date is None:
                raise Exception('MarketCheck status_date is null.')

            diff = max_marketcheck_status_date - max_inventory_status_date
            if diff.total_seconds() > timelapse_seconds:
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