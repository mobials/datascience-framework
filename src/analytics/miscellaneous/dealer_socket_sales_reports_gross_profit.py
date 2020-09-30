import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import os
import postgreshandler
import datetime
import psycopg2
import psycopg2.extras

schema = 'miscellaneous'
script = os.path.basename(__file__)[:-3]

file_paths = [
    '/Users/caseywood/Downloads/Gross Profit Report Red Deer Mitsubishi.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Northland Kia.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Nissan of Nanaimo.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Nissan of Duncan.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Nanaimo Mitsubishi.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Lloydminster Hyundai.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Lethbridge Mitsubishi.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Lethbridge Kia.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Leduc Hyundai.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Kia Red Deer.csv',
    '/Users/caseywood/Downloads/Gross Profit Report GP Mitsubishi.csv',
    '/Users/caseywood/Downloads/Gross Profit Report Cranbrook Kia.csv'
]

insert_query =  '''
                    INSERT INTO 
                        {0}.{1}
                    (
                        dealer,
                        purchase_number,
                        purchase_date,
                        payload
                    )
                    VALUES
                        %s
                    ON CONFLICT 
                    DO NOTHING
                '''.format(schema,script)
with postgreshandler.get_analytics_connection() as connection:
    tuples = []
    for file_path in file_paths:
        print(file_path)
        with open(file_path, "r") as file:
            dealer = None
            headers = None
            line_number = 0
            for line in file.readlines():
                line_number += 1
                print(line_number)
                if file_path == '/Users/caseywood/Downloads/Gross Profit Report Lethbridge Kia.csv' and line_number == 275:
                    h = 1
                if headers is not None and line == '\n':
                    break
                line_split = line.split(',')
                if line_number == 2:
                    dealer = line_split[0]
                elif line_number == 8:
                    # remove the first column, which we'll be using in the primary key
                    headers = line.split(',')
                    headers[len(headers) - 1] = headers[len(headers) - 1].replace('\n', '')
                    headers = headers[1:len(headers)]
                elif line_number > 8:
                    # extract purchase number and purchase date and remove from line_split
                    field_split = line_split[0].split(' ')
                    purchase_number = int(field_split[0])
                    purchase_date = datetime.datetime.strptime(field_split[2], '%m/%d/%Y')
                    line_split = line_split[1:len(line_split)]
                    # remove bad characters
                    for line_split_index in range(len(line_split)):
                        line_split[line_split_index] = line_split[line_split_index].replace('$', '')
                        line_split[line_split_index] = line_split[line_split_index].replace('\n', '')

                    info = dict(zip(headers, line_split))

                    tuple = (
                        dealer,
                        purchase_number,
                        purchase_date,
                        psycopg2.extras.Json(info)
                    )

                    tuples.append(tuple)

    if len(tuples) > 0:
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor,insert_query,tuples)






