import sys
sys.path.insert(0,'../..')
import boto3
import src.settings
import io
import postgreshandler
import json
import os
import datetime
import psycopg2

queries = [
    '''
        CREATE SCHEMA IF NOT EXISTS vendors;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS operations;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS s3;
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_ca_features
        (
            s3_id text,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS operations.scheduler
        (
            schema text not null,
            script text NOT NULL,
            start_date timestamptz NOT NULL,
            frequency interval NOT NULL,
            last_run timestamptz,
            last_update timestamptz,
            status text,
            run_time interval,
            CONSTRAINT scheduler_pk PRIMARY KEY (script)
        );
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency) 
        VALUES ('vendors','marketcheck_ca_features','2020-07-01','15 minute') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.scanned_files 
        (
            id bigserial,
            file text NOT NULL,
            last_modified timestamptz NOT NULL,
            script text not null,
            scanned timestamptz not null,
            CONSTRAINT s3_unq_idx UNIQUE (file,script),
            CONSTRAINT s3_pk PRIMARY KEY (id)
        );
    '''
]

with postgreshandler.get_datascience_connection() as connection:
    for query in queries:
        print(query)
        if len(query) > 0:
            with connection.cursor() as cursor:
                cursor.execute(query)