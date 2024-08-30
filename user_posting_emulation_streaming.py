import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from typing import Dict
import requests
import json
import datetime


random.seed(100)

def json_default(obj):
    """This is to fix json.dumps issues with Decimals and datetimes"""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return str(obj)
        raise TypeError("Object of type '%s' is not JSON serializable" % type(obj).__name__)
    if isinstance(obj, datetime.datetime):
        return obj.__str__()

class AWSDBConnector:

    DATABASE : str
    HOST : str 
    PASSWORD : str 
    USER : str 
    PORT : str

    def __init__(self):

        self.read_db_creds()
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

    def read_db_creds(self,filename='db_creds.yaml') -> None:
        """reads a .yaml with database credentials in it.
        if no filename param set, will default to "db_creds.yaml", in the working directory.

        yml file must be in the following format:

        RDS_HOST: (enter detail here)
        RDS_PASSWORD: (enter detail here)
        RDS_USER: (enter detail here)
        RDS_DATABASE: (enter detail here)
        RDS_PORT: (enter detail here)

        """
        data_loaded : Dict
        with open(filename,'r') as stream:
        #yaml.reader('db')
            data_loaded = yaml.safe_load(stream) # dict
            self.DATABASE = data_loaded['RDS_DATABASE']
            self.HOST = data_loaded['RDS_HOST']
            self.PASSWORD = data_loaded['RDS_PASSWORD']
            self.USER = data_loaded['RDS_USER']
            self.PORT = data_loaded['RDS_PORT']    


new_connector = AWSDBConnector()


def run_infinite_post_data_loop(endpoint='https://f4q1we8e02.execute-api.us-east-1.amazonaws.com/prod/streams/'):
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            #send to kafka instance
            headers = {'Content-Type': 'application/json'}
            for i in [
                [pin_result,'124a514b9149-pin'],
                [geo_result,'124a514b9149-geo'],
                [user_result,'124a514b9149-user']
            ]:
                # print(i)
                #build correct url
                url = endpoint + 'streaming-' + i[1] + '/record' 
                print(url)
                #build the payload
                payload = { 
                    "StreamName" : [1],
                    "Data" : i[0],
                    "PartitionKey" : "partition_1" # hard set because this exmaple doesn't deal with multiple shards
                    }
                json_payload=json.dumps(payload,default=json_default)
                response = requests.request("PUT", url, headers=headers, data=json_payload)
                print(response.status_code)  
                print(response.text)



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


