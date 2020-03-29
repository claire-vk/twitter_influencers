from __future__ import print_function

import json
import urllib
import boto3
import datetime
import pymysql
import logging
import rds_config
import sys
from math import floor

#rds settings
rds_host  = rds_config.rds_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = rds_config.port

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig()

print('Loading function')

s3 = boto3.client('s3')

#RDS connection stuff

server_address = (rds_host, port)
try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key) #gives out dict
        data = response["Body"].read() #gives out string
        text = data.splitlines()[0]
        output = json.loads(text)
        print("Success in loading the object")
        
        hours = int(output["start_time"][0])
        startMins = int(output["start_time"][1])
        startSecs = float(output["start_time"][2])
        totalStopSecs = startSecs + float(output["chunkDuration"]) #adding the chunkDuration(in secs) to the start time
        # stopSecs = totalStopSecs%60
        # stopMins = floor(startMins + (stopSecs/60))
        start_date = "-".join(str(i) for i in output["start_date"])

        objectKey = str(key)
        channelNumber = output["chunk"]
        patientID = str(output["start_date"][0])

        # converting times to dates and times
        sig_start = dateTimeConvertor(hours, startMins, startSecs, start_date)
        sig_stop = endDateTimeConvertor(hours, startMins, totalStopSecs, start_date)

        print("Successfully read the object into variables")
        print("start secs: ", startSecs)

        with conn.cursor() as cur:
            cur = conn.cursor()
            # cur.execute('insert into edfPatientInfo (PatientID, StartTime, StopTime, ObjectKey, ChannelNumber) values(patientID, "2000-04-12 11:26:00", "2000-04-12 12:26:00", objectKey, 4)')
            cur.execute('INSERT INTO edfPatientInfo (PatientID, StartTime, StopTime, ObjectKey, ChannelNumber) VALUES (%s, %s, %s, %s, %s)',(patientID, sig_start, sig_stop, objectKey, channelNumber))
            conn.commit()
            print("Transfer completed...")

        print("Patient ID: ", patientID)
        print("Start time: ", sig_start)
        print("Stop time: ", sig_stop)
        print("Object key: ", objectKey)
        print("Channel number: ", channelNumber)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def dateTimeConvertor(Hours, Minutes, Seconds, start_date):

    date_1 = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = date_1 + datetime.timedelta(hours = Hours, minutes = Minutes, seconds = Seconds)
    return end_date

def endDateTimeConvertor(Hrs, Mins, Secs, start_date):
    Seconds = Secs%60
    Minutes = floor(Mins + (Secs/60))
    # Hours = floor(Hrs + (Minutes/60))

    date_1 = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = date_1 + datetime.timedelta(hours = Hrs, minutes = Minutes, seconds = Seconds)
    return end_date