import pickle
import pyarrow as pa
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder, LabelBinarizer
import torch
from torch import nn

torch.UntypedStorage.from_file()

class MyModel(nn.Module):
    def __init__(self):
        super(MyModel, self).__init__()
        self.fc1 = nn.Linear(None, 512)
        self.fc2 = nn.Linear(512, 128)
        self.fc3 = nn.Linear(128, 5)
        self.relu = nn.ReLU()
        self.softmax = nn.Softmax(dim=1)
        
    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.softmax(self.fc3(x))
        return x

from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    scaler_path = '/workspace/data/data/test_tpch/Q5_standard_scale_model.pkl'
    enc_path = '/workspace/data/data/test_tpch/Q5_one_hot_encoder.pkl'
    lb_path = '/workspace/data/data/test_tpch/Q5_label_binarizer.pkl'
    model_path = '/workspace/data/data/test_tpch/Q5_pytorch_mlp.model'
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    with open(enc_path, 'rb') as f:
        enc = pickle.load(f)
    with open(lb_path, 'rb') as f:
        lb = pickle.load(f)
    mlp = torch.load(model_path)
    mlp.eval()


    def udf(c_acctbal, o_totalprice, l_quantity, l_extendedprice, l_discount, l_tax, s_acctbal,
  o_orderstatus, l_returnflag, l_linestatus, l_shipinstruct, 
  l_shipmode, n_nationkey, n_regionkey):
        

        data = np.column_stack([c_acctbal, o_totalprice, l_quantity, l_extendedprice, l_discount, l_tax, s_acctbal,
    o_orderstatus, l_returnflag, l_linestatus, l_shipinstruct, 
    l_shipmode, n_nationkey, n_regionkey])
        data = np.split(data, np.array([7]), axis = 1)
        numerical = data[0]
        categorical = data[1]
        X = torch.tensor(np.hstack((scaler.transform(numerical), enc.transform(categorical).toarray())), dtype=torch.float32)
        with torch.no_grad():
            predictions = mlp(X)
        res = lb.inverse_transform(predictions.numpy())
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