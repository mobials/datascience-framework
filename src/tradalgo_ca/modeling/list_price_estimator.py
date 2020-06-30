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
import psycopg2
import psycopg2.extras
import copy
import time
import utility
import pytz
import os


script = os.path.basename(__file__)[:-3]

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

list_price_model_insert_query =    '''
                                        INSERT INTO
                                            list_price_models
                                        (
                                            session_id,
                                            year,
                                            make,
                                            model,
                                            trim,
                                            style,
                                            model_info
                                        )
                                        VALUES 
                                            %s
                                    '''

list_price_model_training_data_insert_query =    '''
                                                    INSERT INTO 
                                                        list_price_model_training_data
                                                    (
                                                        vin,
                                                        year,
                                                        make,
                                                        model,
                                                        trim,
                                                        style,
                                                        body_type,
                                                        msrp,
                                                        mileage,
                                                        price,
                                                        vehicles,
                                                        rank,
                                                        session_id,
                                                        status
                                                    )
                                                    VALUES 
                                                        %s
                                                '''

session_info = {
                    'date': str(datetime.datetime.now()),
                    'vehicle_grouping_features': ['year','make','model','trim','style'],
                    'features': ['mileage'],
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

while True:
    print('checking schedule')
    schedule_info = None
    scheduler_connection = postgreshandler.get_tradalgo_canada_connection()
    schedule_info = postgreshandler.get_script_schedule(scheduler_connection, script)
    if schedule_info is None:
        raise Exception('Schedule not found.')
    scheduler_connection.close()

    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    last_run = schedule_info['last_run']
    start_date = schedule_info['start_date']
    frequency = schedule_info['frequency']
    status = schedule_info['status']
    last_update = schedule_info['last_update']
    run_time = schedule_info['run_time']
    next_run = None
    if last_run is None:
        next_run = start_date
    else:
        next_run = utility.get_next_run(start_date, last_run, frequency)

    if now < next_run:
        print('sleeping')
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    print('getting etl connectiono')
    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    etl_connection = postgreshandler.get_tradalgo_canada_connection()
    try:
        print('getting training data')
        session_id = postgreshandler.insert_tradalgo_session(etl_connection, session_info)
        training_set = pandas.read_sql(session_info['training_set_query'], etl_connection)
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
            print(len(modelled_vehicles), len(matched_vehicles), len(modelled_vehicles) + len(matched_vehicles),vehicle_count, total_vehicles)

            vehicles = len(vehicle_data)
            if vehicles < session_info['minimum_vehicles']:
                training_set.loc[vehicle_data.index, 'status'] = status_map['too few vehicles']
                continue

            vehicle_data_outliers = get_outliers(vehicle_data, session_info)
            outlier_indices = set(vehicle_data_outliers.index)

            training_set.loc[outlier_indices, 'status'] = status_map['outlier']

            outlier_count = len(outlier_indices)
            if len(vehicle_data) - outlier_count < session_info['minimum_vehicles']:
                training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map[
                    'too few vehicles']
                continue

            X = vehicle_data.drop(outlier_indices)[session_info['features']]
            y = vehicle_data.drop(outlier_indices)[session_info['target']]
            model = sklearn.linear_model.LinearRegression()
            model.fit(X, y)

            # check if valid model
            if not model.coef_[0] < 0:
                training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map['bad coefficient']
                continue

            if float(model.intercept_[0]) < 0:
                training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map['bad intercept']
                continue

            proportion_lost = -1 * session_info['depreciation_mileage'] * model.coef_[0] / model.intercept_
            if proportion_lost < session_info['minimum_depreciation']:
                training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map[
                    'depreciation too slow']
                continue

            if proportion_lost > session_info['maximum_depreciation']:
                training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map[
                    'depreciation too fast']
                continue

            training_set.loc[vehicle_data.drop(outlier_indices).index, 'status'] = status_map['included']

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
                'year':vehicle_key[0].item(),
                'make':vehicle_key[1],
                'model':vehicle_key[2],
                'trim': vehicle_key[3],
                'style': vehicle_key[4],
                'body_type': vehicle_data.iloc[0]['body_type'],
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
                'tier': 0
            }

            modelled_vehicles[vehicle_key] = model_info

        for vehicle_key, vehicle_data in training_set_grouping:
            print(len(modelled_vehicles), len(matched_vehicles), len(modelled_vehicles) + len(matched_vehicles),vehicle_count, total_vehicles)
            if vehicle_key in modelled_vehicles.keys():
                continue

            # let's try to find a similar vehicle that's already been modelled
            year = vehicle_key[0].item()
            make = vehicle_key[1]
            model = vehicle_key[2]
            trim = vehicle_key[3]
            style = vehicle_key[4]
            body_type = vehicle_data.iloc[0]['body_type']
            msrp = vehicle_data['msrp'].mean()
            matched_key = None
            tier = None
            if not numpy.isnan(msrp):
                # we have msrp
                # find matches on year, make, model, msrp +=
                max_msrp = msrp + msrp * session_info['match_msrp_multiplier']
                min_msrp = msrp - msrp * session_info['match_msrp_multiplier']

                matches = [(key, abs(value['msrp'] - msrp)) for key, value in modelled_vehicles.items()
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
                # no msrp
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
                model_info['match'] = (int(matched_key[0]),matched_key[1],matched_key[2],matched_key[3],matched_key[4])
                matched_vehicles[vehicle_key] = model_info

        modelled_vehicle_tuples = [(
                                        session_id,
                                        int(key[0]),#year
                                        key[1],#make
                                        key[2],#model
                                        key[3],#trim
                                        key[4],#style
                                    psycopg2.extras.Json(value)) for key,value in
                                   modelled_vehicles.items()]
        matched_vehicle_tuples = [(     session_id,
                                        int(key[0]),#year
                                        key[1],#make
                                        key[2],#model
                                        key[3],#trim
                                        key[4],#style,
                                        psycopg2.extras.Json(value)) for key,value in
                                  matched_vehicles.items()]
        list_price_model_tuples = modelled_vehicle_tuples + matched_vehicle_tuples
        print('inserting...')
        if len(list_price_model_tuples) > 0:
            with etl_connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, list_price_model_insert_query, list_price_model_tuples)
                list_price_model_training_data_tuples = list(training_set.itertuples(index=False, name=None))
                psycopg2.extras.execute_values(cursor, list_price_model_training_data_insert_query, list_price_model_training_data_tuples)
                status = 'success'
                last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                run_time = last_update - start_time
        etl_connection.commit()

    except Exception as e:
        status = str(e)
        print(e)
    finally:
        etl_connection.close()
        print('closing')

    #update the scheduler
    scheduler_connection = postgreshandler.get_tradalgo_canada_connection()
    postgreshandler.update_script_schedule(scheduler_connection, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()