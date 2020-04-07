import sys
sys.path.insert(0,'../..')
import psycopg2
import postgreshandler
import os
import boto3
import src

script = os.path.basename(__file__)

dataone_insert_query =  '''
                            INSERT INTO 
                                dataone
                            (
                                s3_id,
                                vin_pattern,
                                vehicle_id,
                                market, 
                                year, 
                                make,
                                model,
                                text,
                                trim,
                                style,
                                body_type,
                                msrp
                            )
                            VALUES 
                                %s
                            ON CONFLICT ON CONSTRAINT dataone_vin_pat_veh_id_idx
                            DO UPDATE 
                            SET 
                                s3_id = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.s3_id else dataone.s3_id end,
                                market = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.market else dataone.market end,
                                year = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.year else dataone.year end,
                                make = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                    then EXCLUDED.make else dataone.make end,
                                model = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.model else dataone.model end,
                                trim = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.trim else dataone.trim end,
                                style = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.style else dataone.style end,
                                body_type = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.body_type else dataone.body_type end,
                                msrp = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                                then EXCLUDED.msrp else dataone.msrp end
                        '''

s3_completed_files = []
with postgreshandler.get_tradalgo_canada_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_dataone_bucket

client = boto3.client('s3')

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object in objects:
    print(object.key)

