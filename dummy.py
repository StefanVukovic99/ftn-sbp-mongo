import pymongo
import pandas as pd
import ast

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["DATABASENAME"]

def insertData(data, name):
    col = db[name]
    col.drop()
    col.insert_many(data)
    print(f"inserted {name}")

def insertCSV(csv, name, jsonCols = []):
    data = pd.read_csv(csv, dtype=str).to_dict('records')
    for document in data:
        for col in jsonCols:
            if(type(document[col]) == str): document[col] = ast.literal_eval(document[col])
    print(f"read {name}")
    insertData(data, name)

def basicInsert():    
    insertCSV('FILENAME.csv', 'COLLECTION_NAME', [])
    
basicInsert()