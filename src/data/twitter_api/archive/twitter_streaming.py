import tweepy
from tweepy import Stream
# from tweepy import OAuthHander
from tweepy.streaming import StreamListener
import time
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
import pymongo 
import ipywidgets as wgt
from IPython.display import display 
from sklearn.feature_extraction.text import CountVectorizer
import re 
from datetime import datetime
import json
import cPickle as pickle
%matplotlib inline
import urllib
import boto3

# Credentials needed to access the API and make requests.
consumer_key = 'xxx'
consumer_secret = 'xxx'
access_token = 'xxx'
access_token_secret = 'xxx' 

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
auth.secure = True
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                 compression=True, retry_count=10, retry_delay=5, retry_errors=5)


#connect to Kinesis

class StreamListener(tweepy.StreamListener):
    def __init__(self, boto_client):
        super(tweepy.StreamListener, self).__init__()
        self.kinesis = boto_client

    def on_status(self, status):
        print status.txt

    def on_data(self, data):
        self.kinesis.put_record(DeliveryStreamName='twitter',
                                Record={'Data': data})

    def on_error(self, status):
        print status
        return False


stream_name = 'project4_capstone_stream'
client = boto3.client('firehose')

stream_status = client.describe_delivery_stream(DeliveryStreamName=stream_name)
if stream_status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
    print "\n ==== KINESES ONLINE ===="

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

streamListener = StreamListener(client)
stream = tweepy.Stream(auth=api.auth, listener=streamListener)

stream.filter(track=['WWDC2017'],
                      languages=['en'])

