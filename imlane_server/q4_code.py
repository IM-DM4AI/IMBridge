import pickle
import pyarrow as pa
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder, LabelBinarizer
import lightgbm as lgb


from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    scaler_path = '/workspace/data/data/test_tpch/Q10_standard_scale_model.pkl'
    enc_path = '/workspace/data/data/test_tpch/Q10_one_hot_encoder.pkl'
    lb_path = '/workspace/data/data/test_tpch/Q10_label_binarizer.pkl'
    model_path = '/workspace/data/data/test_tpch/Q10_lgb_gbdt_model.txt'
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    with open(enc_path, 'rb') as f:
        enc = pickle.load(f)
    with open(lb_path, 'rb') as f:
        lb = pickle.load(f)
    model = lgb.Booster(model_file=model_path)


    def udf(c_acctbal, o_totalprice, l_quantity, l_extendedprice, l_discount, l_tax,
         o_orderstatus, o_orderpriority, l_linestatus,
           l_shipinstruct, l_shipmode, n_nationkey, n_regionkey):
    
        data = np.column_stack([c_acctbal, o_totalprice, l_quantity, l_extendedprice, l_discount, l_tax,
            o_orderstatus, o_orderpriority, l_linestatus,
            l_shipinstruct, l_shipmode, n_nationkey, n_regionkey])
        data = np.split(data, np.array([6]), axis = 1)
        numerical = data[0]
        categorical = data[1]
        X = np.hstack((scaler.transform(numerical), enc.transform(categorical).toarray()))
        res = lb.inverse_transform(model.predict(X))
        return res

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