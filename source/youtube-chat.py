# -*- coding: utf-8 -*-
"""

วิชา BADS 7205 Data Streaming and Real-Time Analytics
ชื่อโครงการ : Real-time Dashboard for YouTube Streamer 

ผู้จัดทำ 
อรรถศิลป์ 	จิตรวิโรจน์ 	6220422077
สรวิศ 		พรหมมาศ 	6220422082
สุเมธ 		เกตุศรี 		6220422087
สุภามาศ 	ธรรมเลอศักดิ์ 	6220422091
ทรงพล 		ตรีสกุลวัฒนา 	6220422092

"""

from time import sleep
from json import dumps
from kafka import KafkaProducer
import pytchat
import re

TOPIC_NAME = 'input_sleepy_youtube'

producer = KafkaProducer(bootstrap_servers=['ec2-65-1-100-39.ap-south-1.compute.amazonaws.com:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# Video ID from youtube link https://www.youtube.com/watch?v=oSi7b2_Ge9M
video_id = "XF0jq50icLI"


# pattern of emoji in the youtube live message for filter out
regexp_emoji = re.compile(':[ña-z_’!0-9]+:')


chat = pytchat.create(video_id=video_id)
while chat.is_alive():
    data = chat.get()
    for c in data.sync_items():  
        c.message = re.sub(regexp_emoji, '', c.message)
        data = f"{video_id}:::{c.author.name}:::{c.message}"
        print(data)
        producer.send(TOPIC_NAME, value=data)

        #---- Write to text file---
        #with open('youtube-live.log.txt', 'a', encoding='utf-8') as f: 
        #    f.write(f'{c.datetime}:::{video_id}:::{c.author.name}:::{c.message}\n')

print('Youtube live chat is ended')