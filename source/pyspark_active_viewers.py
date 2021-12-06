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
sheet = client.open("youtube_live").worksheet('pyspark_active_viewers') # change the cheet name here

sc = SparkContext()
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 60)
kvs = KafkaUtils.createDirectStream(ssc, [kafka_topic_input], {"metadata.broker.list": kafka_bootstrap_servers})

# return video id as a key, username as a value. return None for incorrect input format.
def extract_data(x):
    if not x[1] or len(x[1].split(':::')) < 3:
        return (None, None)
    else:
        return (":::".join(x[1].encode('utf8').decode('unicode_escape').split(':::')[:2])[1:], 1)
 
def update_data(x):
    data = x.collect()
    dt_now = (datetime.now() - datetime(1899, 12, 30))/timedelta(days=1)
    max_rank = 10
    i = 1
    if data: # check if there is data
        for key, c in data: # loop each user
            vid_id, username = key.split(":::") # split key into video id and username
            sheet.append_row([vid_id, username, dt_now, c]) # append row to gg spreadsheet
            i += 1
            if i > max_rank:
                break

input_data = kvs.map(extract_data) # check if it's new line
filtered_data = input_data.filter(lambda x: x[0]) # filter out None
comment_count = filtered_data.reduceByKey(lambda a, b: a + b).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)) # count comments of each user and sort
comment_count.foreachRDD(update_data)

ssc.start()
ssc.awaitTermination()
     