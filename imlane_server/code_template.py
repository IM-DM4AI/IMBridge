import joblib
import pyarrow as pa
import numpy as np
import pandas as pd


from threadpoolctl import threadpool_limits
@threadpool_limits.wrap(limits=1)
def process_table(table):
    data = table.to_pandas().values
    with open('/home/obtest/log/output.log', 'w+') as f:
        print(f"data : {data}",file=f)
        print(f"table : {table}",file=f)
    res = data[:,0] + data[:,1]
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