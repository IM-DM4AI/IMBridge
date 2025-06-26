import pandas as pd
import pyarrow as pa
import pickle
import numpy as np

from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    # with open('/home/obtest/log/output.log', 'a+') as f:
    #     print(f"table : {table}",file=f)
    root_model_path = "/workspace/data/data/test_raven"
    scaler_path = f'{root_model_path}/Flights/flights_standard_scale_model.pkl'
    enc_path = f'{root_model_path}/Flights/flights_one_hot_encoder.pkl'
    model_path = f'{root_model_path}/Flights/flights_rf_model.pkl'
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    with open(enc_path, 'rb') as f:
        enc = pickle.load(f)
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    data = table.to_pandas().values
    data = np.split(data, np.array([4]), axis=1)
    numerical = data[0]
    categorical = data[1]

    X = np.hstack((scaler.transform(numerical),
                  enc.transform(categorical).toarray()))
    res = model.predict(X)
    # with open('/home/obtest/log/output.log', 'a+') as f:
    #     print(f"res : {res}",file=f)
    #     print("\n\n\n",file=f)
    df = pd.DataFrame(res)
    
    return pa.Table.from_pandas(df)
    

class MyProcess:
    def __init__(self):
        # load model part
        pass


    def process(self, table):
        # print(table.num_rows)
        return process_table(table)