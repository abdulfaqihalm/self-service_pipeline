import pandas as pd
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation, performance_metrics
import numpy as np
from random import sample
from datetime import datetime, timedelta

# parameter candidate
param = {
    'fourier_order_ms': [3, 4, 5, 6],    
    'changepoint_prior_scale':
    np.random.uniform(0.01, 0.1, 8).tolist(),
    'seasonality_prior_scale':
    sample(
        np.random.uniform(0.01, 0.05, 5).tolist() +
        np.random.uniform(1, 10, 5).tolist(), 5) + sample(
            np.random.uniform(0.01, 0.05, 5).tolist() +
            np.random.uniform(1, 10, 5).tolist(), 5),
    'n_changepoints':
    np.random.choice(range(5, 26), 10, replace=False).tolist()
}

# number of iteration
MAX_EVALS = 5

def objective(df, hyperparameters, iteration):
    """Objective function for random search. Returns
       the MAPE score from a set of hyperparameters,
       iteration, and model."""

    # Model fitting
    m = Prophet(
        changepoint_prior_scale=hyperparameters['changepoint_prior_scale'],
        seasonality_prior_scale=hyperparameters['seasonality_prior_scale'],
        n_changepoints=hyperparameters['n_changepoints'])
    m.add_country_holidays(country_name='ID')
    m.add_seasonality(name='monthly',
                      period=30.5,
                      fourier_order=hyperparameters['fourier_order_ms'])
    m.fit(df)
    # MAPE
    cv_results = performance_metrics(
        cross_validation(m,
                         initial='{} days'.format(round(len(df) * 0.75)),
                         horizon='{} days'.format(round(len(df) *
                                                        0.1))))['mape'].mean()
    mape = cv_results

    return [mape, hyperparameters, iteration, m]

def random_search(df, param_grid, max_evals=MAX_EVALS):
    """Random search for hyperparameter optimization"""
    results = pd.DataFrame(columns=['mape', 'params', 'iteration', 'model'],
                           index=list(range(MAX_EVALS)))

    for i in range(max_evals):

        # Choose random hyperparameters
        hyperparameters = {k: sample(v, 1)[0] for k, v in param_grid.items()}
        # Evaluate randomly selected hyperparameters
        eval_results = objective(df, hyperparameters, i)
        results.loc[i, :] = eval_results

    # Model without param tuning
    m = Prophet()
    m.fit(df)
    df_cv = cross_validation(m,
                             initial='{} days'.format(round(len(df) * 0.75)),
                             horizon='{} days'.format(round(len(df) * 0.1)))
    df_p = performance_metrics(df_cv)
    mape = df_p.mape.mean()
    results.loc[max_evals, :] = [mape, None, None, m]

    # Sort with best score on top
    results.sort_values('mape', inplace=True)
    results.reset_index(inplace=True)
    return results

def train(df, category_cols=None):
    model = {}
    perf = {}
    par = {}
    if category_cols == None:
        print('#### Forecasting start ###')
        if len(df) >= 30:

            random_results = random_search(df, param)
            model[0] = random_results['model'][0]
            perf[0] = random_results['mape'][0]
            par[0] = random_results['params'][0]
        else:
            print(
                'Your data only have {} datapoint, not enough data to do forecasting'
                .format(len(df)))
    elif category_cols != None:
        df[category_cols] = df[category_cols].astype('category')
        for i in df[category_cols].dtypes.categories.tolist():
            print('#### Forecasting for category {} ####'.format(str(i)))

            if len(df[df[category_cols] == i]) >= 30:

                random_results = random_search(df[df[category_cols] == i],
                                               param)
                model[i] = random_results['model'][0]
                perf[i] = random_results['mape'][0]
                par[i] = random_results['params'][0]

            else:
                print(
                    'Your data only have {} datapoint, not enough data to do forecasting'
                    .format(len(df[df[category_cols] == i])))
    return model

def predict(date, models,schedule_interval,category_cols=None ):
    
    next_date = pd.DataFrame({'ds':pd.date_range(date + timedelta(1), date + timedelta(schedule_interval), freq='D')})
    print(models)
    if category_cols == None:
        result = {}
        result.setdefault('date', {})
        result.setdefault('forecast', {})
        forecast = models.predict(next_date)
        result['forecast'][0] = int(round(forecast.yhat[0]))
        result['date'][0] = date
        return pd.DataFrame.from_dict(result)
    
    elif category_cols != None:
        forecast_result = pd.DataFrame()
        result = {}
        result.setdefault('date', {})
        result.setdefault('forecast', {})
        result.setdefault('category', {})
        for cat, model in models.items():
            print(cat)
            forecast = model.predict(next_date)
            result['date'] = forecast.ds
            result['forecast'] = forecast.yhat.astype('int')
            result['category']= [cat] * len(forecast.ds)
            result_forecast = pd.DataFrame.from_dict(result)
            forecast_result = forecast_result.append(result_forecast, ignore_index=True)
            print(result_forecast)
        return forecast_result
