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

def get_price_outliers(training_set_data,cutoff_ratio):
    avg = numpy.mean(training_set_data.price)
    result = training_set_data[(training_set_data.price > avg*cutoff_ratio)]
    training_set_data['avg_price'] = avg
    training_set_data['cutoff'] = avg*cutoff_ratio
    return result

def get_price_miles_outliers(training_set_data,cutoff_ratio):
    avg = numpy.mean(training_set_data.price / training_set_data.miles)
    result = training_set_data[((training_set_data.price / training_set_data.miles) > avg*cutoff_ratio)]

    training_set_data['avg_price_miles'] = avg
    training_set_data['price_miles'] = training_set_data.price / training_set_data.miles
    return result

def get_msrp_outliers(training_set_data):
    #result = pandas.DataFrame([])
    result = training_set_data[(training_set_data.msrp > 0) & (training_set_data.price > training_set_data.msrp*1.5)]
    return result

def get_elliptic_envelope_outliers(training_set_data, features):
    import sklearn.covariance
    model = sklearn.covariance.EllipticEnvelope(random_state=0)
    fit = model.fit(training_set_data[features])
    predictions = model.predict(training_set_data[features])
    outlier_indices = [i for i, j in enumerate(predictions) if j == -1]
    result = training_set_data.iloc[outlier_indices]
    return result

def get_isolation_forest_outliers(training_set_data,features):
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    forest = sklearn.ensemble.IsolationForest(n_estimators=100,
                                              #contamination=0.1 if len(training_set_data) < 100 else 0.05,
                                              n_jobs=None,
                                              random_state = 1234)
    forest.fit(training_set_data[features])
    predictions = forest.predict(training_set_data[features])
    outlier_indices = [i for i, j in enumerate(predictions) if j == -1]
    result = training_set_data.iloc[outlier_indices]
    return result

def get_outliers(session_info,training_set_data):
    result = pandas.DataFrame([])
    for outlier_detection_method in session_info['outlier_detection_methods']:
        method = outlier_detection_method['method']
        if method == 'sklearn.ensemble.IsolationForest':
            features = outlier_detection_method['features']
            outliers = get_isolation_forest_outliers(training_set_data.drop(result.index),features)
            result = result.append(outliers)
            #x = len(outliers)
        elif method == 'mindev':
            raise NotImplementedError(method)

    return result

#JM1BK32F71 100786282
minimium_vehicles = 20
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


session_info = {
                    'date': str(datetime.datetime.now()),
                    'vehicle_grouping_features': ['year', 'make', 'model', 'trim', 'style'],
                    'features': ['miles'],
                    'target': ['price'],
                    'minimum_vehicles': minimium_vehicles,
                    'maximum_rank': maximum_rank,
                    'training_set_query':training_set_query,
                    'minimum_depreciation': 0.125,
                    'maximum_depreciation': 0.75,
                    'depreciation_mileage': 100000.0,
                    'adjusted_msrp_parameters':
                    {
                        'min_r2': 0.5
                    },
                    'outlier_detection_methods':
                    [
                        {
                            'method': 'sklearn.ensemble.IsolationForest',
                            'features': ['miles', 'price']
                        }
                    ]
                }

with postgreshandler.get_tradalgo_canada_connection() as connection:
    training_set = pandas.read_sql(session_info['training_set_query'], connection)
    training_set_grouping = training_set.groupby(session_info['vehicle_grouping_features'])

    unmodelled_vehicles = pandas.DataFrame([])

    for training_set_key, training_set_data in training_set_grouping:

        #outliers = get_outliers(session_info,training_set_data)

        #outliers = get_elliptic_envelope_outliers(training_set_data,session_info['features'])
        #uncomment this if you want to look at outlier removal

        #outliers = get_msrp_outliers(training_set_data)
        #outliers = get_price_miles_outliers(training_set_data,2.5)

        #outliers = get_isolation_forest_outliers(training_set_data,session_info['features'])

        outliers = get_price_outliers(training_set_data,3)

        #print(training_set_data.vin_pattern)
        #print(training_set_data.vehicle_id)
        categories = []
        for index in training_set_data.index:
            if outliers.index.contains(index):
                categories.append(1)
            else:
                categories.append(0)

        matplotlib.pyplot.scatter(
            training_set_data[session_info['features']].values.flatten(),
            training_set_data[session_info['target']].values.flatten(),
            c = numpy.array(categories)
        )

        m, b = numpy.polyfit(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), training_set_data.drop(outliers.index)[session_info['target']].values.flatten(), 1)

        matplotlib.pyplot.plot(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), m * training_set_data.drop(outliers.index)[session_info['features']].values.flatten() + b)
        matplotlib.pyplot.show()
        h = 1


        #normalize distance
