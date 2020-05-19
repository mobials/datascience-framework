import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import numpy
numpy.random.seed(1234)
import sklearn
import sklearn.ensemble
import sklearn.linear_model
import postgreshandler
import datetime
import pandas
import warnings
import matplotlib
import matplotlib.pyplot
import pyod


minimium_vehicles = 100
maximum_rank = 1000000

training_set_query =    '''
                            SELECT 
                                *
                            FROM
                                v_training_set
                            WHERE 
                                vehicles >= {0}
                            AND 
                                rank <= {1}
                        '''.format(minimium_vehicles,maximum_rank)

with postgreshandler.get_tradalgo_canada_connection() as connection:
    training_set = pandas.read_sql(training_set_query, connection)
    training_set_grouping = training_set.groupby(['year', 'make', 'model', 'trim', 'style'])

    for training_set_key, training_set_data in training_set_grouping:

        categories = []
        for index in training_set_data.index:
            if training_set_data.loc[index]['rank'] > minimium_vehicles:
                categories.append(1)
            else:
                categories.append(0)

        matplotlib.pyplot.scatter(
            training_set_data['miles'].values.flatten(),
            training_set_data['price'].values.flatten(),
            c = numpy.array(categories)
        )

        matplotlib.pyplot.show()
        h = 1

