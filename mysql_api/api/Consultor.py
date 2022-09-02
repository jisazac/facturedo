import pandas as pd
import boto3 
import awswrangler as wr
from .commands import run_athena_query



DATABASE="facturedo"
ACCESS_KEY='AKIA4755WFGJAXMIO2VR'
SECRET_KEY='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'
REGION_NAME='us-east-1'
WORKGROUP="primary"
S3_OUTPUT="s3://facturedobucket/data/"



session_dict={
'region_name':REGION_NAME,
'aws_access_key_id':ACCESS_KEY,
'aws_secret_access_key':SECRET_KEY,
}


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