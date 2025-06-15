import warnings
import joblib
import pyarrow as pa
import numpy as np
import pandas as pd
from statsmodels.tools.sm_exceptions import ValueWarning
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from threadpoolctl import threadpool_limits

class UseCase03Model(object):
    def __init__(self, use_store=False, use_department=True):
        if not use_store and not use_department:
            raise ValueError(f"use_store = {use_store}, use_department = {use_department}: at least one must be True")

        self._use_store = use_store
        self._use_department = use_department
        self._models = {}
        self._min = {}
        self._max = {}

    def _get_key(self, store, department):
        if self._use_store and self._use_department:
            key = (store, department)
        elif self._use_store:
            key = store
        else:
            key = department

        return key

    def store_model(self, store: int, department: int, model, ts_min, ts_max):
        key = self._get_key(store, department)
        self._models[key] = model
        self._min[key] = ts_min
        self._max[key] = ts_max

    def get_model(self, store: int, department: int):
        key = self._get_key(store, department)
        model = self._models[key]
        ts_min = self._min[key]
        ts_max = self._max[key]
        return model, ts_min, ts_max

@threadpool_limits.wrap(limits=1)
def process_table(table):
    scale = 40
    name = "uc03"
    root_model_path = f"/workspace/data/data/tpcxai_datasets/sf{scale}"
    model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
    model = joblib.load(model_file_name)

    def udf(store, department):
        forecasts = []
        data = pd.DataFrame({
            'store': store,
            'department': department
        })
        # print(data.shape)
        # combinations = np.unique(data[['Store', 'Dept']].values, axis=0)
        for index, row in data.iterrows():
            store = row.store
            dept = row.department
            periods = 52
            try:
                current_model, ts_min, ts_max = model.get_model(store, dept)
            except KeyError:
                continue
            # disable warnings that non-date index is returned from forecast
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=ValueWarning)
                forecast = current_model.forecast(periods)
                forecast = np.clip(forecast, a_min=0.0, a_max=None)  # replace negative forecasts
            start = pd.date_range(ts_max, periods=2)[1]
            forecast_idx = pd.date_range(start, periods=periods, freq='W-FRI')
            forecasts.append(
                str({'store': store, 'department': dept, 'date': forecast_idx, 'weekly_sales': forecast})
            )
        return np.array(forecasts)
    df = pd.DataFrame(udf(*table))
    # print(len(df))
    return pa.Table.from_pandas(df)

class MyProcess:
    def __init__(self):
        # load model part
        pass

    def process(self, table):
        # print(table.num_rows)
        return process_table(table)
