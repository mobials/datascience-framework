import sys
sys.path.append('/var/www/datascience-framework/src/')
import settings
import datetime
import psycopg2
import psycopg2.extras
import csv

def get_dashboard_connection():
    connection_string = {
        "dbname": settings.dashboard_database,
        "user": settings.dashboard_username,
        "password": settings.dashboard_password,
        "host": settings.dashboard_host,
        "port": settings.dashboard_port
    }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_datascience_connection():
    connection_string = {
                            "dbname": settings.datascience_db,
                            "user": settings.datascience_user,
                            "password": settings.datascience_pass,
                            "host": settings.datascience_host,
                            "port":settings.datascience_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_analytics_connection():
    connection_string = {
                            "dbname": settings.analytics_database,
                            "user": settings.analytics_username,
                            "password": settings.analytics_password,
                            "host": settings.analytics_host,
                            "port":settings.analytics_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_tradalgo_staging_connection():
    connection_string = {
                            "dbname": settings.tradalgo_staging_db,
                            "user": settings.tradalgo_staging_user,
                            "password": settings.tradalgo_staging_pass,
                            "host": settings.tradalgo_staging_host,
                            "port": settings.tradalgo_staging_port
                        }
    connection = psycopg2.connect(**connection_string)
    return connection

def get_tradalgo_canada_connection():
    connection_string = {
        "dbname": settings.tradalgo_canada_username,
        "user": settings.tradalgo_canada_username,
        "password": settings.tradalgo_canada_password,
        "host": settings.tradalgo_canada_host,
        "port": settings.tradalgo_canada_port
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

def get_s3_scanned_max_last_modified_date(connection,script):
     query =    '''
                    SELECT
                        last_modified
                    FROM
                        s3
                    WHERE
                        script = %(script)s
                    ORDER BY 
                        last_modified DESC
                    LIMIT 1
                '''
     result = None
     with connection.cursor() as cursor:
         parameters = {'script': script}
         cursor.execute(query, parameters)
         row = cursor.fetchone()
         if row is not None:
             result = row[0]
     return result

def get_s3_scanned_files(connection, script):
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
    import os
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

def insert_tradalgo_session(connection,session_info):
    query = '''
                    INSERT INTO
                        sessions
                    (
                        session_info
                    )
                    VALUES
                    (
                        %(session_info)s
                    )
                    RETURNING 
                        id
                '''
    _session_info = psycopg2.extras.Json(session_info)
    with connection.cursor() as cursor:
        cursor.execute(query, {'session_info': _session_info})
        result = cursor.fetchone()
        if result is not None and result[0] is not None:
            result = result[0]
            return result

def get_script_schedule(connection,script):
    query = '''
                SELECT 
                    *
                FROM
                    scheduler
                WHERE
                    script = %(script)s
            '''

    with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
        cursor.execute(query,{'script':script})
        result = cursor.fetchone()
        if result is not None:
            return result

def update_script_schedule(connection,script,last_run,status,run_time,last_update):
    query = '''
                UPDATE
                    scheduler
                SET 
                    last_run = %(last_run)s,
                    status = %(status)s,
                    run_time = %(run_time)s,
                    last_update = %(last_update)s
                WHERE 
                    script = %(script)s
            '''
    with connection.cursor() as cursor:
        cursor.execute(query,{'script':script,'last_run':last_run,'status':status,'run_time':run_time,'last_update':last_update})

def update_zuora_table(connection,table,data):
    print(table)
    insert_query =  '''
                        INSERT INTO 
                            zuora_{0}
                        (
                            id, 
                            updateddate,
                            payload 
                        )
                        VALUES 
                            %s
                    '''.format(table)

    delete_query =  '''
                        DELETE FROM
                            zuora_{0}
                        WHERE 
                            id 
                        IN 
                            %s
                    '''.format(table)

    reader = csv.DictReader(data.splitlines())
    ids = []
    tuples = []
    for row in reader:
        id = row['{0}.Id'.format(table)]
        ids.append(id)
        if row['{0}.is_deleted'.format(table)] == 'true':
            continue

        updateddate = datetime.datetime.strptime(row['{0}.UpdatedDate'.format(table)],'%Y-%m-%dT%H:%M:%S%z')
        del row['{0}.UpdatedDate'.format(table)]
        del row['{0}.Id'.format(table)]
        payload = psycopg2.extras.Json(row)

        tuple = (
            id,
            updateddate,
            payload
        )

        tuples.append(tuple)

    if len(ids) > 0:
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, delete_query, (ids,))

    if len(tuples) > 0:
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, insert_query, tuples)



