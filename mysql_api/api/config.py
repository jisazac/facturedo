import os
import boto3


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

database_args ={
    "workgroup":WORKGROUP,
    "s3_output":S3_OUTPUT,
    "database":DATABASE

}

#string=f"mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}"
