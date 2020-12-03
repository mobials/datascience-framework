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
import pandas
import numpy

schema = 'public'
script = os.path.basename(__file__)[:-3]

read_query =    '''
                    with data_set as (
                    select distinct on (a.taxonomy_vin,a.vin)
                        a.taxonomy_vin,
                        a.vin,
                        a.miles,
                        (b.payload->>'YEAR')::int as year,
                        (b.payload->>'MAKE')::text as make,
                        count(*) OVER (PARTITION BY b.payload->>'YEAR', b.payload->>'MAKE') AS vehicles
                    from 
                        vendors.marketcheck_ca a,
                        vendors.dataone b 
                    where
                        b.vin_pattern = a.taxonomy_vin
                    and 
                        a.status_date >= (select max(status_date) from vendors.marketcheck_ca) - interval %(time_window)s
                    and 
                        a.miles >= %(minimum_miles)s
                    and 
                        a.miles < %(maximum_miles)s
                    and 
                        b.vin_pattern not in    (
                                                    select
                                                        vin_pattern
                                                    from 
                                                        vendors.dataone
                                                    group by 
                                                        1
                                                    having 
                                                        count(distinct payload->>'YEAR') > 1
                                                )
                    order by  
                        a.taxonomy_vin,a.vin,a.status_date desc)
                    select * from data_set where vehicles >= %(minimum_vehicles)s                
                '''

insert_query =  '''
                    INSERT INTO {0}.{1}
                    (
                        session_id,
                        vin_pattern,
                        region,
                        mileage 
                    )
                    VALUES 
                        %s
                '''.format(schema,script)

vehicle_grouping_parameters = ['year','make']
minimum_vehicles = 50
p_minimum_difference = .10
#regions = ['NL','PE','NS','NB','QC','ON','MB','SK','AB','BC','YT','NT','NU']
regions = ['']
minimum_miles = 500
maximum_miles = 300000
time_window = '10 days'
payload =   {
                'date': str(datetime.datetime.now()),
                'vehicle_grouping_parameters':vehicle_grouping_parameters,
                'p_minimum_difference':p_minimum_difference,
                'regions':regions,
                'minimum_miles':minimum_miles,
                'maximum_miles':maximum_miles,
                'minimum_vehicles':minimum_vehicles,
                'time_window':time_window
            }

postgres_connection = postgreshandler.get_analytics_connection()
session_id = postgreshandler.create_session(postgres_connection,schema,script,payload)
with postgres_connection.cursor() as cursor:
    data = pandas.read_sql(read_query, postgres_connection,params={'time_window':time_window,
                                                                       'minimum_miles':minimum_miles,
                                                                       'maximum_miles':maximum_miles,
                                                                       'minimum_vehicles':minimum_vehicles})
    vehicle_grouping = data.groupby(vehicle_grouping_parameters)

    tuples = []
    #iterate over each group
    for vehicle_key, vehicle_data in vehicle_grouping:
        mean = numpy.mean(vehicle_data['miles'])
        mileage = int(mean * (1 - p_minimum_difference))
        for vin_pattern in list(vehicle_data['taxonomy_vin'].unique()):
            for region in regions:
                tuple = (
                    session_id,
                    vin_pattern,
                    region,
                    mileage
                )
                tuples.append(tuple)

    if len(tuples) > 0:
        with postgres_connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor,insert_query,tuples)

    postgres_connection.commit()

print('finished')