import pandas as pd
import config
from config import session_dict
from config import database_args
import boto3 
import awswrangler as wr
from utils import run_athena_query



#MySqlModel=Mysql_Model(config_dict)       


#boto3.Session(region_name='us-east-1',aws_access_key_id=config_dict["ACCESS_KEY"],aws_secret_access_key=config_dict["SECRET_KEY"])
input={"client_id":1}

class AWS_Model:
    def __init__(self,session_dict,input):
        self.session_dict=session_dict
        self.input=input

    def set_session(self) -> object:
        dictt=self.session_dict
        return boto3.Session(**dictt)

    def makeQuery(self):
        dict_id=self.input
        ID=dict_id["client_id"]
        query=f'SELECT * FROM "dataset_v4" WHERE "id_deudor" = {ID};'
        return query
    
    def bd_to_dataframe(self) -> object:
        
        dataframe=run_athena_query(self.makeQuery(),boto3_session=self.set_session(),region_name="us-east-1")
        return dataframe


if __name__ == "__main__":
    api=AWS_Model(session_dict,input)      
    df=api.bd_to_dataframe()
    print(df.head())

#MySqlModel=Mysql_Model(config_dict)  