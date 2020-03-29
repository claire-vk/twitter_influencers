import os
import uuid
import tweepy
import boto3
import json
from pprint import pprint

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


def main():
    '''This program uses AWS's resource client to control the Kinesis-firehose
    instance'''

    # Set up enviromentals
    # Credentials needed to access the API and make requests.
    consumer_key = 'xxx'
    consumer_secret = 'xxx'
    access_token = 'xxx'
    access_token_secret = 'xxx' 
    stream_name = 'project4_capstone_stream'

    # Instanciate client
    client = boto3.client('firehose',region_name='us-east-2')

    # Get status of the delivery stream
    stream_status = client.describe_delivery_stream(DeliveryStreamName=stream_name)
    if stream_status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
        print "\n ==== KINESES ONLINE ===="



    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    auth.secure = True
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                     compression=True, retry_count=10, retry_delay=5, retry_errors=5)


    # auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    # auth.set_access_token(access_token, access_secret)
    # api = tweepy.API(auth)

    streamListener = StreamListener(client)
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)

    try:
        stream.filter(track=['WWDC2017'],
                      languages=['en'])
    finally:
        stream.disconnect()


if __name__ == '__main__':
    main()