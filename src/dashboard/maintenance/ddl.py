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

file_path = '../sql/tables.sql'

with postgreshandler.get_dashboard_connection() as connection:
    with open(file_path,'r') as file:
        text = file.read().replace('\n', '')
        queries = text.split(';')
        for query in queries:
            print(query)
            if len(query) > 0:
                with connection.cursor() as cursor:
                    cursor.execute(query)
