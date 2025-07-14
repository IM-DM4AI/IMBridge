import requests
import pandas as pd
import pyarrow as pa

ip_addres = "http://172.23.166.101:8000"
q_id = 7


def process_table(table):
    data = table.to_pandas()
    data = pd.DataFrame({
        'smart_5_raw': data.iloc[:,0],
        'smart_10_raw': data.iloc[:,1],
        'smart_184_raw': data.iloc[:,2],
        'smart_187_raw': data.iloc[:,3],
        'smart_188_raw': data.iloc[:,4],
        'smart_197_raw': data.iloc[:,5],
        'smart_198_raw': data.iloc[:,6]
    })
    response = requests.post(ip_addres, json={"data": data.to_json(orient="split"), "query": q_id})
    res = response.json()
    res_df = pd.read_json(res["res"], orient="split")
    # print(len(res_df))
    return pa.Table.from_pandas(res_df)
    

class MyProcess:
    def __init__(self):
        # load model part
        pass


    def process(self, table):
        # print(table.num_rows)
        return process_table(table)