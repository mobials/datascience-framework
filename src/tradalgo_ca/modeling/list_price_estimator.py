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
import copy

def get_msrp_outliers(vehicle_data,multiplier):
    result = vehicle_data[(vehicle_data.msrp > 0) & (vehicle_data.price > vehicle_data.msrp * multiplier)]
    #vehicle_data.loc[vehicle_data[(vehicle_data.msrp > 0) & (vehicle_data.price > vehicle_data.msrp * multiplier)].index, 'status'] = status_map['outlier']
    return result

def get_price_outliers(vehicle_data,multiplier):
    avg = numpy.mean(vehicle_data.price)
    result = vehicle_data[(vehicle_data.price > (avg * multiplier))]
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

# def plot_outliers(training_set,session_info):
#     categories = []
#     for index in training_set_data.index:
#         if outliers.index.contains(index):
#             categories.append(1)
#         else:
#             categories.append(0)
#
#     matplotlib.pyplot.scatter(
#         training_set_data[session_info['features']].values.flatten(),
#         training_set_data[session_info['target']].values.flatten(),
#         c = numpy.array(categories)
#     )
#
#     m, b = numpy.polyfit(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), training_set_data.drop(outliers.index)[session_info['target']].values.flatten(), 1)
#
#     matplotlib.pyplot.plot(training_set_data.drop(outliers.index)[session_info['features']].values.flatten(), m * training_set_data.drop(outliers.index)[session_info['features']].values.flatten() + b)
#     matplotlib.pyplot.show()

maximum_vehicles = 100
minimium_vehicles = 15

training_set_query =    '''
                            SELECT 
                                *
                            FROM
                                v_training_set
                            WHERE
                                rank <= {0}
                        '''.format(maximum_vehicles)

#training_set_query = '''select * from m_training_set_hyundai'''

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
                                                        vin,
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
                    ],
                    'match_msrp_multiplier':0.05
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
    modelled_keys = []

    modelled_vehicles = {}
    matched_vehicles = {}
    vehicle_count = 0
    for vehicle_key, vehicle_data in training_set_grouping:
        vehicle_count += 1
        print(len(modelled_vehicles),len(matched_vehicles),len(modelled_vehicles) + len(matched_vehicles), vehicle_count,total_vehicles)

        vehicles = len(vehicle_data)
        if vehicles < session_info['minimum_vehicles']:
            training_set.loc[vehicle_data.index,'status'] = status_map['too few vehicles']
            continue

        vehicle_data_outliers = get_outliers(vehicle_data,session_info)
        outlier_indices = set(vehicle_data_outliers.index)

        training_set.loc[outlier_indices,'status'] = status_map['outlier']

        outlier_count = len(outlier_indices)
        if len(vehicle_data) - outlier_count < session_info['minimum_vehicles']:
            training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['too few vehicles']
            continue

        X = vehicle_data.drop(outlier_indices)[session_info['features']]
        y = vehicle_data.drop(outlier_indices)[session_info['target']]
        model = sklearn.linear_model.LinearRegression()
        model.fit(X, y)

        #check if valid model
        if not model.coef_[0] < 0:
            training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['bad coefficient']
            continue

        if  float(model.intercept_[0]) < 0:
            training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['bad intercept']
            continue

        proportion_lost = -1 * session_info['depreciation_mileage'] * model.coef_[0] / model.intercept_
        if proportion_lost < session_info['minimum_depreciation']:
            training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['depreciation too slow']
            continue

        if proportion_lost > session_info['maximum_depreciation']:
            training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['depreciation too fast']
            continue

        training_set.loc[vehicle_data.drop(outlier_indices).index,'status'] = status_map['included']

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
            'year':vehicle_data.iloc[0]['year'].item(),
            'make': vehicle_data.iloc[0]['make'],
            'model':vehicle_data.iloc[0]['model'],
            'trim': vehicle_data.iloc[0]['trim'],
            'style': vehicle_data.iloc[0]['style'],
            'body_type':vehicle_data.iloc[0]['body_type'],
            'coefficients': coef_dict,
            'intercept': float(model.intercept_[0]),
            'r2': r2,
            'mean_squared_error': mse,
            'mean_absolute_error': mae,
            'size': size,
            'msrp': msrp if not numpy.isnan(msrp) else None,
            'mean_observed_list_price': mean_observed_list_price,
            'mean_predicted_list_price': mean_predicted_list_price,
            'minimum_list_price': float(minimum_list_price),
            'maximum_list_price': float(maximum_list_price),
            'tier':0
        }

        modelled_vehicles[vehicle_key] = model_info

    for vehicle_key,vehicle_data in training_set_grouping:
        print(len(modelled_vehicles),len(matched_vehicles),len(modelled_vehicles) + len(matched_vehicles), vehicle_count,total_vehicles)
        if vehicle_key in modelled_vehicles.keys():
            continue

        #let's try to find a similar vehicle that's already been modelled
        year = vehicle_data.iloc[0]['year'].item()
        make = vehicle_data.iloc[0]['make']
        model = vehicle_data.iloc[0]['model']
        trim = vehicle_data.iloc[0]['trim']
        body_type = vehicle_data.iloc[0]['body_type']
        style = vehicle_data.iloc[0]['style']
        msrp = vehicle_data['msrp'].mean()

        matched_key = None
        tier = None
        if not numpy.isnan(msrp):
            #we have msrp
            #find matches on year, make, model, msrp +=
            max_msrp = msrp + msrp*session_info['match_msrp_multiplier']
            min_msrp = msrp - msrp*session_info['match_msrp_multiplier']

            matches = [(key, abs(value['msrp']-msrp)) for key, value in modelled_vehicles.items()
                       if
                           value['msrp'] is not None and
                           value['year'] == year and
                           value['make'] == make and
                           value['model'] == model and
                           value['msrp'] <= max_msrp and
                           value['msrp'] >= min_msrp
                       ]

            if len(matches) > 0:
                min_diff = min([x[1] for x in matches])
                min_diff_index = [x[1] for x in matches].index(min_diff)
                matched_key = matches[min_diff_index][0]
                tier = 1
            else:
                matches = [(key, abs(value['msrp'] - msrp)) for key, value in modelled_vehicles.items()
                           if
                           value['msrp'] is not None and
                           value['year'] == year and
                           value['make'] == make and
                           value['body_type'] == body_type and
                           value['msrp'] <= max_msrp and
                           value['msrp'] >= min_msrp
                           ]

                if len(matches) > 0:
                    min_diff = min([x[1] for x in matches])
                    min_diff_index = [x[1] for x in matches].index(min_diff)
                    matched_key = matches[min_diff_index][0]
                    tier = 2
                else:
                    matches = [(key, abs(value['msrp'] - msrp)) for key, value in modelled_vehicles.items()
                               if
                               value['msrp'] is not None and
                               value['year'] == year - 1 and
                               value['make'] == make and
                               value['model'] == model and
                               value['trim'] == trim and
                               value['msrp'] <= max_msrp and
                               value['msrp'] >= min_msrp
                               ]

                    if len(matches) > 0:
                        min_diff = min([x[1] for x in matches])
                        min_diff_index = [x[1] for x in matches].index(min_diff)
                        matched_key = matches[min_diff_index][0]
                        tier = 3
                    else:
                        matches = [(key, abs(value['msrp'] - msrp)) for key, value in modelled_vehicles.items()
                                   if
                                   value['msrp'] is not None and
                                   value['year'] == year and
                                   value['body_type'] == body_type and
                                   value['msrp'] <= max_msrp and
                                   value['msrp'] >= min_msrp
                                   ]

                        if len(matches) > 0:
                            min_diff = min([x[1] for x in matches])
                            min_diff_index = [x[1] for x in matches].index(min_diff)
                            matched_key = matches[min_diff_index][0]
                            tier = 5
        else:
            #no msrp
            matches = [key for key, value in modelled_vehicles.items()
                       if
                       value['year'] == year and
                       value['make'] == make and
                       value['model'] == model and
                       value['trim'] == trim
                       ]

            if len(matches) > 0:
                matched_key = matches[0]
                tier = 4

        if matched_key is not None:
            model_info = copy.deepcopy(modelled_vehicles[matched_key])
            model_info['tier'] = tier
            model_info['match'] = (matched_key[0],int(matched_key[1]))
            matched_vehicles[vehicle_key] = model_info

    modelled_vehicle_tuples = [(session_id,key[0],int(key[1]),psycopg2.extras.Json(value)) for key,value in modelled_vehicles.items()]
    matched_vehicle_tuples = [(session_id,key[0],int(key[1]),psycopg2.extras.Json(value)) for key,value in matched_vehicles.items()]
    list_price_model_tuples = modelled_vehicle_tuples + matched_vehicle_tuples
    print('inserting...')
    if len(list_price_model_tuples) > 0:
        list_price_model_training_data_tuples = list(training_set[['session_id','vin_pattern','vehicle_id','dataone_s3_id','cdc_s3_id','vin','status']].itertuples(index=False,name=None))
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, list_price_model_insert_query, list_price_model_tuples)
            psycopg2.extras.execute_values(cursor, list_price_model_training_data_insert_query,list_price_model_training_data_tuples)

print('finished')

