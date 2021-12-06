# -*- coding: utf-8 -*-
"""
Created on Sun Nov 21 22:34:40 2021

@author: DELL
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
from datetime import datetime, timedelta
import findspark
findspark.init('C:/spark-2.4.8-bin-hadoop2.7')
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

kafka_topic_input = "input_sleepy_youtube"
kafka_bootstrap_servers = 'ec2-65-1-100-39.ap-south-1.compute.amazonaws.com:9092'

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
cerds = ServiceAccountCredentials.from_json_keyfile_name("youtubelive.json", scope)
client = gspread.authorize(cerds)
sheet = client.open("youtube_live").worksheet('pyspark_spam') # change the cheet name here

sc = SparkContext()
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 10)
kvs = KafkaUtils.createDirectStream(ssc, [kafka_topic_input], {"metadata.broker.list": kafka_bootstrap_servers})

data_logger = sc.broadcast({}) # {video_id: {username: count}} == count number of times in a row that user enters comments too frequently

# return video id as a key, username as a value. return None for incorrect input format.
def extract_data(x):
    if not x[1] or len(x[1].split(':::')) < 3:
        return (None, None)
    else:
        return (":::".join(x[1].encode('utf8').decode('unicode_escape').split(':::')[:2])[1:], 1)
 
def find_spam(x):
    data = x.collect()
    user_list = {} # to store all potential spam users in this time window
    
    if data: # check if there is data
        for key, c in data: # loop each user
            vid_id, username = key.split(":::") # split key into video id and username
            if vid_id not in user_list:
                user_list[vid_id] = set()
            # check if it's new video id. if yes, create new key
            if vid_id not in data_logger.value:
                data_logger.value[vid_id] = {}
            if c > 4:
                user_list[vid_id].add(username)
                    
                if username not in data_logger.value[vid_id]:
                    data_logger.value[vid_id][username] = 1
                else:
                    data_logger.value[vid_id][username] += 1
                    
                if data_logger.value[vid_id][username] == 1:
                    sheet.append_row([vid_id, username]) # append row to gg spreadsheet if this user is potential spam

        # decrease the number of times in a row that user enters comments too frequently for users who do not exist in this time window
        #for vid_id in user_list:
        #    temp_user_list = set()
        #    for usr in data_logger.value[vid_id]:
        #        if usr not in user_list[vid_id] and data_logger.value[vid_id][usr] > 0 and data_logger.value[vid_id][usr] < 2:
        #            data_logger.value[vid_id][usr] -= 1
        #            if data_logger.value[vid_id][usr] == 0:
        #                temp_user_list.add(usr)
        #    for usr in temp_user_list:
        #        data_logger.value[vid_id].pop(usr)
        print(datetime.now(), data_logger.value)

input_data = kvs.map(extract_data) # check if it's new line
filtered_data = input_data.filter(lambda x: x[0]) # filter out None
comment_count = filtered_data.reduceByKey(lambda a, b: a + b) # count comments of each user
comment_count.foreachRDD(find_spam) # detect spam users based on specified criteria

ssc.start()
ssc.awaitTermination()
     