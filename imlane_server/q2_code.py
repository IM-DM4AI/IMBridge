import pandas as pd
import pyarrow as pa
import pickle
import numpy as np

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from xgboost import XGBClassifier
from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    root_model_path = "/workspace/data/data/test_raven"
    scaler_path = f'{root_model_path}/Expedia/expedia_standard_scale_model.pkl'
    enc_path = f'{root_model_path}/Expedia/expedia_one_hot_encoder.pkl'
    model_path = f'{root_model_path}/Expedia/expedia_gbdt_model.pkl'
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    with open(enc_path, 'rb') as f:
        enc = pickle.load(f)
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    data = table.to_pandas().values
    data = np.split(data, np.array([8]), axis=1)
    numerical = scaler.transform(data[0])
    categorical = enc.transform(data[1]).toarray()
    X = np.hstack((numerical,
                   categorical))
    res = model.predict(X)
    df = pd.DataFrame(res)
    # print(len(df))
    return pa.Table.from_pandas(df)
    

class MyProcess:
    def __init__(self):
        # load model part
        pass


    def process(self, table):
        # print(table.num_rows)
        return process_table(table)