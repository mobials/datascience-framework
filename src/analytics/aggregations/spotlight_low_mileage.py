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

schema = 'autoverify'
script = os.path.basename(__file__)[:-3]

read_query =    '''
                    select distinct on (a.taxonomy_vin,a.vin)
                        a.taxonomy_vin,
                        a.vin,
                        a.miles,
                        (b.payload->>'YEAR')::int as year,
                        (b.payload->>'MAKE')::text as make
                    from 
                        vendors.marketcheck_ca a,
                        vendors.dataone b 
                    where
                        b.vin_pattern = a.taxonomy_vin
                    and 
                        a.status_date >= now() - interval '3 days'
                    and 
                        a.miles >= 500
                    and 
                        a.miles < 300000
                    order by  
                        a.taxonomy_vin,a.vin,a.status_date desc                 
                '''
vehicle_grouping_parameters = ['year','make']
minimum_vehicles = 100

postgres_etl_connection = postgreshandler.get_analytics_connection()
with postgres_etl_connection.cursor() as cursor:
    data = pandas.read_sql(read_query, postgres_etl_connection)
    vehicle_grouping = data.groupby(vehicle_grouping_parameters)

    h = 1