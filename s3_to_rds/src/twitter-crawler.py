#!/usr/bin/env python3

import json
import urllib.parse
import boto3
import pymysql

RDS_ENDPOINT = 'xxx'
RDS_USERNAME = 'xxx'
RDS_PASSWORD = 'xxx'
RDS_DATABASE = 'xxx'

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        jsondata = [json.loads(data) for data in response['Body'].read().decode('utf-8').strip().split('\n')]

        print(jsondata)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    # put data into RDS database
    insertsql = 'INSERT INTO stream (rt_status_created_at,rt_status_favorite_count,rt_status_favorited,rt_status_geo,rt_status_id,'\
                                     'rt_status_in_reply_to_status_id_num,rt_status_in_reply_to_user_id_str_num,rt_status_retweet_count,'\
                                     'rt_status_retweeted,rt_status_source,rt_status_text,rt_status_truncated,rt_status_users_created_at,'\
                                     'rt_status_users_favourites_count,rt_status_users_followers_count,rt_status_users_following,'\
                                     'rt_status_users_friends_count,rt_status_users_id,rt_status_users_lang,rt_status_users_listed_count,'\
                                     'rt_status_users_location,rt_status_users_screen_name,rt_status_users_statuses_count,rt_status_users_time_zone,'\
                                     'rt_status_users_verified,status_created_at,status_favorite_count,status_geo,'\
                                     'status_id,status_in_reply_to_status_id_num,status_in_reply_to_user_id_str_num,status_num_hashtags,status_num_mentions,'\
                                     'status_retweet_count,status_retweeted,status_text,status_truncated,user_created_at,user_desc,'\
                                     'user_favorite_count,user_followers_count,user_friends_count,user_id,user_listed_count,user_loc,user_name,'\
                                     'user_screen_name,user_statuses_count,user_time_zone,user_url) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    with pymysql.connect(RDS_ENDPOINT, user=RDS_USERNAME, passwd=RDS_PASSWORD, db=RDS_DATABASE, connect_timeout=5) as db:
        for data in jsondata:
          db.execute(insertsql, args=(  data['rt_status_created_at'],
                                        data['rt_status_favorite_count'],
                                        data['rt_status_favorited'],
                                        data['rt_status_geo'],
                                        data['rt_status_id'],
                                        data['rt_status_in_reply_to_status_id_num'],
                                        data['rt_status_in_reply_to_user_id_str_num'],
                                        data['rt_status_retweet_count'],
                                        data['rt_status_retweeted'],
                                        data['rt_status_source'],
                                        data['rt_status_text'],
                                        data['rt_status_truncated'],
                                        data['rt_status_users_created_at'],
                                        data['rt_status_users_favourites_count'],
                                        data['rt_status_users_followers_count'],
                                        data['rt_status_users_following'],
                                        data['rt_status_users_friends_count'],
                                        data['rt_status_users_id'],
                                        data['rt_status_users_lang'],
                                        data['rt_status_users_listed_count'],
                                        data['rt_status_users_location'],
                                        data['rt_status_users_screen_name'],
                                        data['rt_status_users_statuses_count'],
                                        data['rt_status_users_time_zone'],
                                        data['rt_status_users_verified'],
                                        data['status_created_at'],
                                        data['status_favorite_count'],
                                        data['status_geo'],
                                        data['status_id'],
                                        data['status_in_reply_to_status_id_num'],
                                        data['status_in_reply_to_user_id_str_num'],
                                        data['status_num_hashtags'],
                                        data['status_num_mentions'],
                                        data['status_retweet_count'],
                                        data['status_retweeted'],
                                        data['status_text'],
                                        data['status_truncated'],
                                        data['user_created_at'],
                                        data['user_desc'],
                                        data['user_favorite_count'],
                                        data['user_followers_count'],
                                        data['user_friends_count'],
                                        data['user_id'],
                                        data['user_listed_count'],
                                        data['user_loc'],
                                        data['user_name'],
                                        data['user_screen_name'],
                                        data['user_statuses_count'],
                                        data['user_time_zone'],
                                        data['user_url']))

    return None
