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
import psycopg2
import psycopg2.extras

#pandas.options.mode.chained_assignment = None

def get_msrp_outliers(vehicle_data,multiplier):
    result = vehicle_data[(vehicle_data.msrp > 0) & (vehicle_data.price > vehicle_data.msrp * multiplier)]
    #vehicle_data.loc[vehicle_data[(vehicle_data.msrp > 0) & (vehicle_data.price > vehicle_data.msrp * multiplier)].index, 'status'] = status_map['outlier']
    return result

def get_price_outliers(vehicle_data,multiplier):
    avg = numpy.mean(vehicle_data.price)
    result = vehicle_data[(vehicle_data.price > (avg * multiplier))]
    #vehicle_data.loc[(vehicle_data.price > (avg * multiplier)),'status'] = status_map['outlier']
    return result

def get_elliptic_envelope_outliers(training_set_data, features):
    import sklearn.covariance
    model = sklearn.covariance.EllipticEnvelope(random_state=0)
    fit = model.fit(training_set_data[features])
    predictions = model.predict(training_set_data[features])
    outlier_indices = [i for i, j in enumerate(predictions) if j == -1]
    result = training_set_data.iloc[outlier_indices]
    return result

def get_outliers(training_set,session_info):
    result = pandas.DataFrame([])
    for outlier_detection_method in session_info['outlier_methods']:
        method = outlier_detection_method['method']
        if method == 'price':
            outliers = get_price_outliers(training_set, outlier_detection_method['multiplier'])
            result = result.append(outliers)
        elif method == 'msrp':
            outliers = get_msrp_outliers(training_set, outlier_detection_method['multiplier'])
            result = result.append(outliers)
        else:
            raise NotImplementedError(method)
    return result


maximum_vehicles = 200000
minimium_vehicles = 20

training_set_query =    '''
                            SELECT 
                                *
                            FROM
                                v_training_set
                            --LIMIT 362
                        '''

list_price_model_insert_query =    '''
                                        INSERT INTO
                                            list_price_models
                                        (
                                            session_id,
                                            vin_pattern,
                                            vehicle_id,
                                            model_info
                                        )
                                        VALUES 
                                            %s
                                    '''

list_price_model_training_data_insert_query =    '''
                                                    INSERT INTO
                                                        list_price_model_training_data
                                                    (
                                                        session_id,
                                                        vin_pattern,
                                                        vehicle_id,
                                                        dataone_s3_id,
                                                        cdc_s3_id,
                                                        status
                                                    )
                                                    VALUES 
                                                        %s
                                                '''

session_info = {
                    'date': str(datetime.datetime.now()),
                    'vehicle_grouping_features': ['vin_pattern','vehicle_id'],
                    'features': ['miles'],
                    'target': ['price'],
                    'minimum_vehicles': minimium_vehicles,
                    'maximum_vehicles': maximum_vehicles,
                    'training_set_query':training_set_query,
                    'minimum_depreciation': 0.125,
                    'maximum_depreciation': 0.75,
                    'depreciation_mileage': 100000.0,
                    'outlier_methods':
                    [
                        {
                            'method': 'msrp',
                            'multiplier':1.5
                        },
{
                            'method': 'price',
                            'multiplier':3
                        }
                    ]
                }

status_map = {
    'too few vehicles':1,
    'outlier':2,
    'included':3,
    'bad coefficient':4,
    'bad intercept':4,
    'depreciation too fast':5,
    'depreciation too slow':6
}

with postgreshandler.get_tradalgo_canada_connection() as connection:
    session_id = postgreshandler.insert_tradalgo_session(connection,session_info)
    training_set = pandas.read_sql(session_info['training_set_query'], connection)
    training_set['session_id'] = session_id
    training_set['status'] = None
    training_set_grouping = training_set.groupby(session_info['vehicle_grouping_features'])
    total_vehicles = len(training_set_grouping)
    list_price_model_tuples = []

    modelled_count = 0
    vehicle_count = 0
    for vehicle_key, vehicle_data in training_set_grouping:
        vehicle_count += 1
        print(modelled_count,vehicle_count,total_vehicles)

        vehicles = len(vehicle_data)
        if vehicles < session_info['minimum_vehicles']:
            training_set.loc[vehicle_data.index,'status'] = status_map['too few vehicles']
            continue

        vehicle_data_outliers = get_outliers(vehicle_data,session_info)
        outlier_indices = set(vehicle_data_outliers.index)

        training_set.loc[outlier_indices,'status'] = status_map['outlier']

        outlier_count = len(outlier_indices)
        if len(vehicle_data) - outlier_count < session_info['minimum_vehicles']:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['too few vehicles']
            continue

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
        # m, b = numpy.polyfit(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), training_set_data.drop(outliers.index)[session_info['target']].values.flatten(), 1)
        #
        # matplotlib.pyplot.plot(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), m * training_set_data.drop(outliers.index)[session_info['features']].values.flatten() + b)
        # matplotlib.pyplot.show()
        # h = 1
        # continue

        X = vehicle_data.drop(outlier_indices)[session_info['features']]
        y = vehicle_data.drop(outlier_indices)[session_info['target']]
        model = sklearn.linear_model.LinearRegression()
        model.fit(X, y)

        #check if valid model
        if not model.coef_[0] < 0:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['bad coefficient']
            continue

        if  float(model.intercept_[0]) < 0:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['bad intercept']
            continue

        proportion_lost = -1 * session_info['depreciation_mileage'] * model.coef_[0] / model.intercept_
        if proportion_lost < session_info['minimum_depreciation']:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['depreciation too slow']
            continue

        if proportion_lost > session_info['maximum_depreciation']:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['depreciation too fast']
            continue

        if outlier_count > 0:
            training_set.loc[~vehicle_data.index.isin(outlier_indices),'status'] = status_map['included']
        else:
            training_set.loc[vehicle_data.index,'status'] = status_map['included']

        coef_dict = {}
        for coef, feat in zip(model.coef_[0, :], session_info['features']):
            coef_dict[feat] = coef

        observed = vehicle_data.drop(outlier_indices)[session_info['target']]
        predicted = model.predict(X)
        r2 = model.score(X, y)
        mse = sklearn.metrics.mean_squared_error(observed, predicted)
        mae = sklearn.metrics.mean_absolute_error(observed, predicted)
        msrp = vehicle_data['msrp'].mean()
        mean_observed_list_price = float(numpy.mean(observed))
        mean_predicted_list_price = numpy.mean(predicted)
        size = len(vehicle_data) - outlier_count
        minimum_list_price = observed.min()
        maximum_list_price = observed.max()

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
                'minimum_list_price': float(minimum_list_price),
                'maximum_list_price': float(maximum_list_price)
            }
        }

        list_price_model_tuples.append(
            (
                session_id,
                vehicle_key[0],
                int(vehicle_key[1]),
                psycopg2.extras.Json(model_info),
            )
        )

        modelled_count += 1

    print('inserting...')
    if len(list_price_model_tuples) > 0:
        list_price_model_training_data_tuples = list(training_set[['session_id','vin_pattern','vehicle_id','dataone_s3_id','cdc_s3_id','status']].itertuples(index=False,name=None))
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, list_price_model_insert_query, list_price_model_tuples)
            psycopg2.extras.execute_values(cursor, list_price_model_training_data_insert_query,list_price_model_training_data_tuples)

print('finished')
