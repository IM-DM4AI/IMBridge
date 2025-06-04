import joblib
import numpy as np
import pyarrow as pa
import pandas as pd
import joblib
from surprise import SVD
from surprise import Dataset
from surprise.reader import Reader


from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    scale = 40
    name = "uc07"
    root_model_path = f"/workspace/data/data/tpcxai_datasets/sf{scale}"
    model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
    model = joblib.load(model_file_name)

    def udf(user_id, item_id):
        ratings = []
        for i in range(len(user_id)):
            rating = model.predict(user_id[i], item_id[i]).est
            ratings.append(rating)
        return np.array(ratings)
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
