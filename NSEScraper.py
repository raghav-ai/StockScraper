import requests
import numpy as np
import pandas as pd
import json
import pymongo
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
from io import StringIO
import csv
import os 

def convert_to_iso(date_str):
   input_format = "%d-%b-%Y"
   try:
       date_object = datetime.strptime(date_str, input_format)
       return date_object.date().isoformat()
   except ValueError:
       return date_str
   
myclient = pymongo.MongoClient("mongodb+srv://*****:*****@cluster0.6ipivxa.mongodb.net/")
mydb= myclient["NSE"]
Metadata = mydb["StockMetaData"]
mycol1 = mydb['RawStockData']

header = {
    "Connection": "keep-alive",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/111.0.0.0 Safari/537.36",
    "Sec-Fetch-User": "?1", "Accept": "*/*", "Sec-Fetch-Site": "none", "Sec-Fetch-Mode": "navigate",
    "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "en-US,en;q=0.9,hi;q=0.8"
}

def nse_urlfetch(url):
    r_session = requests.session()
    return r_session.get(url, headers=header)

today = date.today()
yesterday = today - timedelta(days = 1)
yesterday_str=yesterday.strftime("%d-%m-%Y")
today_str=today.strftime("%d-%m-%Y")

url = "https://www.nseindia.com/api/historical/securityArchives?"
all_stocks = Metadata.find({}) # 1955 stocks
for rows in all_stocks:
  add_stock = []
  if rows['Date'] == "01-01-2023":
    start_year = 2023
    if '&' in rows['Symbol']:
      newSymbol = rows['Symbol'].replace('&', '%26')
      payload = f"from=01-01-"+ str(start_year) +"&to=31-12-"+str(start_year)+"&symbol="+ newSymbol + "&dataType=priceVolumeDeliverable&series=ALL&csv=true"  
    else:
      payload = f"from=01-01-"+ str(start_year) +"&to=31-12-"+str(start_year)+"&symbol="+rows['Symbol']+ "&dataType=priceVolumeDeliverable&series=ALL&csv=true"
    data_text = nse_urlfetch(url + payload)
    data = data_text.text.encode('latin1').decode('utf-8-sig')
    df = pd.read_csv(StringIO(data)) 
    for index,row in df.iterrows():
      add_stock.append({"Symbol":rows['Symbol'],"Date": row['Date  '],"Open": row['Open Price  '], "High":row['High Price  '] ,"Low":row['Low Price  '] ,"Close":row['Close Price  '] ,"Volume":row['Total Traded Quantity  ']})
    if add_stock:
      mycol1.insert_many(add_stock)
      add_stock=[]
    start_year=start_year+1
    Metadata.update_one({'Symbol': rows['Symbol']},{'$set': { 'Date': "01-01-"+str(start_year)}})
  else:
    if '&' in rows['Symbol']:
      newSymbol = rows['Symbol'].replace('&', '%26')
      payload = f"from="+ rows['Date'] +"&to="+yesterday_str+"&symbol="+newSymbol+ "&dataType=priceVolumeDeliverable&series=ALL&csv=true"
    else:
      payload = f"from="+ rows['Date'] +"&to="+yesterday_str+"&symbol="+rows['Symbol']+ "&dataType=priceVolumeDeliverable&series=ALL&csv=true"
    data_text = nse_urlfetch(url + payload)
    data = data_text.text.encode('latin1').decode('utf-8-sig')
    df = pd.read_csv(StringIO(data))
    for index,row in df.iterrows():
      add_stock.append({"Symbol":rows['Symbol'],"Date": row['Date  '],"Open": row['Open Price  '], "High":row['High Price  '] ,"Low":row['Low Price  '] ,"Close":row['Close Price  '] ,"Volume":row['Total Traded Quantity  ']})
    if add_stock:
      mycol1.insert_many(add_stock)
      add_stock=[]
    Metadata.update_one({'Symbol': rows['Symbol']},{'$set': { 'Date': today_str}})

os.makedirs('stock_data',exist_ok=True)
raw_stocks = mycol1.find({})
count  = 0#could be used to debug the program
for row in all_stocks:
  lst = []
  count +=1
  raw_stocks = mycol1.find({"Symbol":row['Symbol']})
  df1 = pd.DataFrame(list(raw_stocks))
  df1['Date'] = df1['Date'].apply(convert_to_iso)
  df1 = df1.sort_values(by='Date',ascending=True)
  del df1['_id']
  del df1['Symbol']
  df1.to_csv('stock_data/'+row['Symbol']+'.csv', index = False)# stock_data/ can be replaced with new directory