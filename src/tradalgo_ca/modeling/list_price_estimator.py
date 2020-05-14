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


def get_elliptic_envelope_outliers(trainin_set_data, features):
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

minimum_vehicles = 15

training_set_query =    '''
                            SELECT 
                                *
                            FROM
                                v_training_set
                            WHERE 
                                vehicles >= {0}
                        '''.format(minimum_vehicles)

training_set_query =    '''
                            SELECT 
                                *
                            FROM
                                scratch.m_training_set_test
                        '''

session_info = {
                    'date': str(datetime.datetime.now()),
                    'vehicle_grouping_features': ['year', 'make', 'model', 'trim', 'style'],
                    'features': ['miles'],
                    'target': ['price'],
                    'minimum_vehicle_group_size': minimum_vehicles,
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

        year = training_set_data.iloc[0]['year']
        make = training_set_data.iloc[0]['make']
        model = training_set_data.iloc[0]['model']
        trim = training_set_data.iloc[0]['trim']
        style = training_set_data.iloc[0]['style']

        outliers = get_outliers(session_info,training_set_data)

        #uncomment this if you want to look at outlier removal

        # categories = []
        # for index in training_set_data.index:
        #     if outliers.index.contains(index):
        #         categories.append(1)
        #     else:
        #         categories.append(0)
        #
        # matplotlib.pyplot.scatter(
        #     training_set_data[session_info['features']].values.flatten(),
        #     training_set_data[session_info['target']].values.flatten(),
        #     c = numpy.array(categories)
        # )
        #
        # matplotlib.pyplot.show()

        X = training_set_data.drop(outliers.index)[session_info['features']]
        y = training_set_data.drop(outliers.index)[session_info['target']]
        model = sklearn.linear_model.LinearRegression()
        model.fit(X, y)

        #check if valid model
        proportion_lost = -1 * session_info['depreciation_mileage'] * model.coef_[0] / model.intercept_
        if proportion_lost < session_info['minimum_depreciation']:
            unmodelled_vehicles = unmodelled_vehicles.append(training_set_data.drop(outliers.index))
            continue
        if proportion_lost > session_info['maximum_depreciation']:
            unmodelled_vehicles = unmodelled_vehicles.append(training_set_data.drop(outliers.index))
            continue
        if  float(model.intercept_[0]) < 0:
            unmodelled_vehicles = unmodelled_vehicles.append(training_set_data.drop(outliers.index))
        if float(model.intercept_[0]) < 0:
            continue

        coef_dict = {}
        for coef, feat in zip(model.coef_[0, :], session_info['features']):
            coef_dict[feat] = coef

        observed = training_set_data.drop(outliers.index)[session_info['target']]
        predicted = model.predict(X)
        r2 = model.score(X, y)
        mse = sklearn.metrics.mean_squared_error(observed, predicted)
        mae = sklearn.metrics.mean_absolute_error(observed, predicted)
        msrp = training_set_data['msrp'].mean()
        mean_observed_list_price = float(numpy.mean(observed))
        mean_predicted_list_price = numpy.mean(predicted)
        size = len(training_set_data.drop(outliers.index))
        minimum_list_price = training_set_data.drop(outliers.index)['list_price'].min()
        maximum_list_price = training_set_data.drop(outliers.index)['list_price'].max()

        model_info = {
            'coefficients': coef_dict,
            'intercept': float(model.intercept_[0]),
            'stats': {
                'r2': r2,
                'mean_squared_error': mse,
                'mean_absolute_error': mae,
                'size': size,
                'msrp': msrp if not numpy.isnan(msrp) else None,
                'mean_observed_list_price': mean_observed_list_price,
                'mean_predicted_list_price': mean_predicted_list_price,
                'minimum_list_price': minimum_list_price,
                'maximum_list_price': maximum_list_price
            }
        }

        vehicle_info = {}
        for vehicle_grouping_feature_index in range(len(session_info['vehicle_grouping_features'])):
            vehicle_grouping_feature = session_info['vehicle_grouping_features'][vehicle_grouping_feature_index]
            vehicle_info[vehicle_grouping_feature] = training_set_key[vehicle_grouping_feature_index] \
                if isinstance(training_set_key[vehicle_grouping_feature_index], str) \
                else training_set_key[vehicle_grouping_feature_index].item()
