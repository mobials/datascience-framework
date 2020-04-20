import psycopg2
from src.settings import *
import datetime

def get_dashboard_connection():
    connection_string = {
        "dbname": dashboard_database,
        "user": dashboard_username,
        "password": dashboard_password,
        "host": dashboard_host,
        "port": dashboard_port
    }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_datascience_connection():
    connection_string = {
                            "dbname": datascience_db,
                            "user": datascience_user,
                            "password": datascience_pass,
                            "host": datascience_host,
                            "port":datascience_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_analytics_connection():
    connection_string = {
                            "dbname": analytics_database,
                            "user": analytics_username,
                            "password": analytics_password,
                            "host": analytics_host,
                            "port":analytics_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_tradalgo_staging_connection():
    connection_string = {
                            "dbname": tradalgo_staging_db,
                            "user": tradalgo_staging_user,
                            "password": tradalgo_staging_pass,
                            "host": tradalgo_staging_host,
                            "port": tradalgo_staging_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_tradalgo_canada_connection():
    connection_string = {
        "dbname": tradalgo_canada_username,
        "user": tradalgo_canada_username,
        "password": tradalgo_canada_password,
        "host": tradalgo_canada_host,
        "port": tradalgo_canada_port
    }
    connection = psycopg2.connect(**connection_string)
    return connection


def get_s3_versions(connection,script):
    query = '''
                SELECT 
                    version_id
                FROM
                    scratch.s3
                WHERE 
                    script = %(script)s
            '''
    with connection.cursor() as cursor:
        cursor.execute(query,{'script':script})
        result = cursor.fetchone()
        if result is not None:
            result = result[0]
        return result

def get_s3_completed_files(connection,script):
    query = '''
                SELECT 
                    file
                FROM
                    s3
                WHERE 
                    script = %(script)s;
            '''
    with connection.cursor() as cursor:
        parameters = {'script':script}
        cursor.execute(query,parameters)
        for row in cursor.fetchall():
            yield row[0]

def insert_s3_file(connection, script, file, last_modified):
    query = '''
                INSERT INTO
                    s3
                (
                    file,
                    last_modified,
                    script,
                    scanned
                )
                VALUES
                (
                    %(file)s,
                    %(last_modified)s,
                    %(script)s,
                    %(scanned)s
                )
                RETURNING id;
            '''
    data = {
            'file':file,
            'last_modified':last_modified,
            'script':script,
            'scanned':datetime.datetime.utcnow()
    }

    with connection.cursor() as cursor:
        cursor.execute(query,data)
        result = cursor.fetchone()
        if result is not None:
            return result[0]

def create_create_table_statement(schema, table, column_names):
    column_definitions = ''

    for column_index in range(len(column_names)):
        column_definitions = column_definitions + column_names[column_index] + ' text' + (',' if column_index < len(column_names) - 1 else '')

    result = '''
                CREATE TABLE {0}.{1} 
                (
                    {2}
                )
            '''.format(schema, table, column_definitions)

    return result


def create_insert_query(schema, table, column_names):

    result =  '''
                            INSERT INTO 
                                {0}.{1}
                            (
                                {2}
                            ) 
                            VALUES 
                                %s
                        '''.format(schema,table,','.join(column_names))

    return result


def create_table_from_list(connection, schema, table, column_names):
    query = create_create_table_statement(schema,table,column_names)
    with connection.cursor() as cursor:
        cursor.execute(query)


def create_table_from_delimited_file(connection,schema,file_path,delimiter=',',quote_character='"'):
    import csv
    with open(file_path) as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quote_character)
        column_names = None
        tuples = []
        insert_query = None
        for row in reader:
            if not column_names:
                column_names = row
                file_name, file_extention = os.path.splitext(file_path)
                base_file_name = os.path.basename(file_path)
                file_name = base_file_name[:-len(file_extention)].lower()
                create_table_from_list(connection, schema, file_name, column_names)
                insert_query = create_insert_query(schema, file_name, column_names)
            else:
                tuples.append(tuple(row))
                print(len(tuples))


        if len(tuples) > 0:
            with connection.cursor() as cursor:
                print('inserting data')
                psycopg2.extras.execute_values(cursor, insert_query, tuples)


