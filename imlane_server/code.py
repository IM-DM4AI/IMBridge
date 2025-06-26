import joblib
import pyarrow as pa
import numpy as np
import pandas as pd
from imblearn.over_sampling import ADASYN
from imblearn.pipeline import Pipeline as Imb_Pipeline
from sklearn import metrics
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC

from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    scale = 40
    name = "uc06"
    root_model_path = f"/workspace/data/data/tpcxai_datasets/sf{scale}"
    model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
    model = joblib.load(model_file_name)
    def udf(smart_5_raw,
            smart_10_raw,
            smart_184_raw,
            smart_187_raw,
            smart_188_raw,
            smart_197_raw,
            smart_198_raw):
        data = pd.DataFrame({
            'smart_5_raw': smart_5_raw,
            'smart_10_raw': smart_10_raw,
            'smart_184_raw': smart_184_raw,
            'smart_187_raw': smart_187_raw,
            'smart_188_raw': smart_188_raw,
            'smart_197_raw': smart_197_raw,
            'smart_198_raw': smart_198_raw
        })
        # print(data.shape)
        return model.predict(data)
    df = pd.DataFrame(udf(*table))
    with open("/home/obtest/log/output_test.log", "a+") as f:
        f.write(f"cnmd {len(df)}\n")
    # print(len(df))
    return pa.Table.from_pandas(df)

class MyProcess:
    def __init__(self):
        # load model part
        pass

    def process(self, table):
        # print(table.num_rows)
        return process_table(table)
