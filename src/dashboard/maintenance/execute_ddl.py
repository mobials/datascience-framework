import src.postgreshandler
import psycopg2

file_path = '../ddl/relations.sql'

with src.postgreshandler.get_dashboard_connection() as connection:
    with open(file_path,'r') as file:
        text = file.read().replace('\n', '')
        queries = text.split(';')
        for query in queries:
            if len(query) > 0:
                with connection.cursor() as cursor:
                    cursor.execute(query)
