import sys
sys.path.append('../..')
sys.path.append('/var/www/datascience-framework/src/')
import pathlib
import os
import settings
import postgreshandler
import psycopg2

file_path = None
cwd = os.getcwd()
if cwd.startswith('/Users/caseywood/'):
    file_path = '../ddl/relations.ddl'
else:
    file_path = '/var/www/datascience-framework/src/dashboard/ddl/relations.ddl'

with postgreshandler.get_dashboard_connection() as connection:
    with open(file_path,'r') as file:
        text = file.read().replace('\n', '')
        queries = text.split(';')
        for query in queries:
            if len(query) > 0:
                with connection.cursor() as cursor:
                    cursor.execute(query)
