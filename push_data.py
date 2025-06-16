import os
import sys
import json
import certifi
import pandas as pd
import numpy as np
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from dotenv import load_dotenv
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
print(f"Using MONGO_URI: {MONGO_URI}")

ca = certifi.where()

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def csv_to_json_converter(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def insert_data_to_mongo(self, records, database, collection):
        try:
            self.database = database
            self.records = records
            self.collection = collection

            self.client = pymongo.MongoClient(MONGO_URI)
            self.db = self.client[self.database]
            self.collection = self.db[self.collection]

            self.collection.insert_many(self.records)
            logging.info(f"Data inserted successfully into {self.database}.{self.collection.name}")
            return (len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
if __name__ == "__main__":
    FILE_PATH = 'Network_Data\phisingData.csv'
    DATABASE = 'rebeldb'
    COLLECTION = 'NetworkData'
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_converter(FILE_PATH)
    no_of_records = networkobj.insert_data_to_mongo(
        records,
        DATABASE,
        COLLECTION
    )
    print(f"Number of records inserted: {no_of_records}")
    logging.info(f"Records inserted: {no_of_records} into {DATABASE}.{COLLECTION}")