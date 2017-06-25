#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import urllib
from google.cloud import language 
import time
import json


#        replace mysql.server with "localhost" if you are running via your own server!
#                        server       MySQL username	MySQL pass  Database name.
# conn = MySQLdb.connect("mysql.server","beginneraccount","cookies","beginneraccount$tutorial")
# c = conn.cursor()

ckey = 'xxx'
csecret = 'xxx'
atoken = 'xxx'
asecret = 'xxx'

##Using an API that requires an authorization key
#sendexAuth = ''


def sentimentAnalysis(text):
	client = language.Client() 
	document =  client.document_from_text(text)
	sent_analysis = document.analyze_sentiment()
	print(dir(sent_analysis))
	sentiment = sent_analysis.sentiment
	ent_analysis = document.analyze_entities()
	entities = ent_analysis.entities
	return sentiment, entities 

	# Code to use another API 
    #enocoded_text = urllib.quote(text)
    #API_Call ='http://sentdex.com/api/api.php?text='+encoded_text+'&auth='+sentdexAuth
    #output = urllib.urlopen(API_Call).read()  
    #return output
        
        
class listener(StreamListener):

    def on_data(self,data):
        try:
        	# Accessing Aspects of  
        	all_data = json.loads(data)
        	tweet_created_at = all_data["created_at"]
        	tweet_id = all_data["id"] #may want to use id_str
        	tweet = all_data["text"].encode('ascii', 'ignore')
        	sentiment, entities  = sentimentAnalysis(tweet)
        	sentScore =  sentiment.score
        	sentMag   =  sentiment.magnitude
        	user_id =  all_data["user"]["id"] #may want to use id_str
        	user_name = all_data["user"]["name"]
        	user_screen_name  =  all_data["user"]["screen_name"]
        	userloc   =  all_data["user"]["location"]
        	userdesc   =  all_data["user"]["description"]
        	userfollowers_count  = all_data["user"]["followers_count"]
        	userfriends_count = all_data["user"]["friends_count"]
        	userlisted_count  = all_data["user"]["listed_count"]
        	userfavorite_count = all_data["user"]["favourites_count"]
        	userstatuses_count = all_data["user"]["statuses_count"]
        	usercreated_at  = all_data["user"]["created_at"]
        	entities_hashtags_length = len(all_data["entities"]["hashtags"]) # we may want to change 
        	entities_urls_length = len(all_data["entities"]["urls"])

        	print "The length of tweet {}, sentscore {}, sentMag {}".format(len(tweet), sentScore, sentMag)
        	# the unixtimestamp
        	
        	#Saving to a File
        	saveThis = str(time.time()) + ':::' + tweet + ':::' + str(sentScore) + ':::' + str(sentMag) + ':::' +username
        	saveFile = open('twitDB.csv','a')
        	saveFile.write(saveThis)
        	saveFile.write('\n')
        	saveFile.close()
        	# writing to a Database
        	
        	return True

        except BaseException, e: 
            print 'failed ondata,',str(e)
            time.sleep(5)
            
    def on_error(self, status):
        if status_code==420:
            return False
        print status
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())
twitterStream.filter(track=["WWDC2017"],stall_warnings=True)