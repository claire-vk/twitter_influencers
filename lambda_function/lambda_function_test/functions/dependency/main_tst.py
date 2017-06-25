from __future__ import print_function
import boto3
import logging
import os
import sys
import uuid
import pymysql
import csv
#import rds_config


rds_host  = 'flaskest.csjkhjjygutf.us-east-1.rds.amazonaws.com'
name = 'flask'
password = 'wizjysys'
db_name = 'flaskdb'


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

s3_client = boto3.client('s3')

def handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['bucket']['key'] 
    # download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    # s3_client.download_file(bucket, key,download_path)
    # o = open(download_path,'rU')
    # csv_data = csv.reader(o)


    try:
        response = s3_client.get_object(Bucket=bucket, Key=key) #gives out dict
        data = response["Body"].read() #gives out string
        text = data.splitlines()[0]
        output = json.loads(text)
        print("Success in loading the object")

        user_id = str(output['user_id'])
        status_id = str(output['status_id'])


        with conn.cursor() as cur:
            cur = conn.cursor()
            # cur.execute('insert into edfPatientInfo (PatientID, StartTime, StopTime, ObjectKey, ChannelNumber) values(patientID, "2000-04-12 11:26:00", "2000-04-12 12:26:00", objectKey, 4)')
            cur.execute('INSERT INTO tst (user_id,status_id) VALUES (%s, %s)',(user_id,status_id))
            conn.commit()
            print("Transfer completed...")

        print("user_id: ", user_id)
        print("status_id: ", status_id)
    

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e







    # with conn.cursor() as cur:
    #     for idx, row in enumerate(csv_data):

    #         logger.info(row)
    #         try:
    #             cur.execute('INSERT INTO flaskdb.test(ids,sex, po,age,degree)'\
    #                             'VALUES(%s,%s, %s, %s,%s)'
    #                             , (row))
    #         except Exception as e:
    #             logger.error(e)

    #         if idx % 100 == 0:
    #             conn.commit()

    #     conn.commit()

    # return 'File loaded into RDS:' + str(download_path)

