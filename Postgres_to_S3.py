#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 08:25:39 2022

@author: kjaswanth
"""

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import pandas as pd
import io
import boto3
import os

engine = create_engine("postgresql://kjaswanth:akira92A@localhost:5432/kjaswanth")


Access_key_ID = 'AKIAYGABTP2T7HK2GDXT'
Secret_access_key =  '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'

query = "SELECT * FROM kjaswanth.data_bank.customer_transactions;"
df = pd.read_sql(query, engine)
print(df)

tbl = 'customer_transactions'

# load data to postgres

try:
    rows_imported = 0
    print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
    upload_file_bucket = 'jaswanthpractise'
    upload_file_key = 'public/' + str(tbl) + f"/{str(tbl)}"
    filepath =  upload_file_key + ".csv"
        #
    s3_client = boto3.client('s3', aws_access_key_id=Access_key_ID, aws_secret_access_key=Secret_access_key,region_name='ap-south-1')
    with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(df)
            print("Data imported successful")
except Exception as e:
        print("Data load error: " + str(e))
        
        
        