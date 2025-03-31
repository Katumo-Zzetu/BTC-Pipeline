import requests
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

url='https://api.polygon.io/v1/open-close/crypto/BTC/USD/2025-03-31?adjusted=true&apiKey=YbyWoQUQTr5oZLnYmEFvgpK04tyShU19'

response = requests.get(url)

# check if request is successful
if response.status_code == 200:
  data = response.json()
  open_price = data.get('open')
  close_price = data.get('close')
  date=data.get('day')

  symbol= data.get('symbol')
#   conn = psycopg2.connect()
#   print(f"Open price on 2025-03-31: ${open_price}")
#   print(f"Close price on 2025-03-31: ${close_price}")
else:
  print(f"Failed to retrieve data:" "{response.status}")

data_df = {
  'symbol': symbol,
  'open_price': open_price,
  'close_price': close_price,
  'date': date
}

df = pd.DataFrame(data_df, index=[0])
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

load_dotenv()

dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

try:
    df.to_sql("crypto_prices", con = engine, if_exists="append", index=False, schema="dataengineering")
    print(f"Successfully loaded crypto data for {df[date]}")
except Exception as e:
    print(f"Error: {e}")
