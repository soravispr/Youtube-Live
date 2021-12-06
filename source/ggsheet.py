# -*- coding: utf-8 -*-
"""
Created on Sun Nov 21 22:34:40 2021

@author: DELL
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
from datetime import datetime, timedelta

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
cerds = ServiceAccountCredentials.from_json_keyfile_name("youtubelive.json", scope)
client = gspread.authorize(cerds)
sheet = client.open("youtube_live").worksheet('pyspark_active_viewers') # change the cheet name here

# data = sheet.get_all_records()  # การรับรายการของระเบียนทั้งหมด
# pprint(data)

for i in range(1, 11):
    dt_now = (datetime.now() - datetime(1899, 12, 30))/timedelta(days=1)
    sheet.append_row([f'id_{i}',f'user_{i}',dt_now,i])