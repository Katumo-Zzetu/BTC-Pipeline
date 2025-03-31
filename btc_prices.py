import requests
import os
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Define API endpoint
url = 'https://api.polygon.io/v1/open-close/crypto/BTC/USD/2025-03-31?adjusted=true&apiKey=YOUR_API_KEY'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    open_price = data.get('open')
    close_price = data.get('close')
    date = data.get('day')
    symbol = data.get('symbol')
else:
    print(f"Failed to retrieve data: {response.status}")
    exit()

# Prepare data for insertion
data_df = {
    'symbol': symbol,
    'open_price': open_price,
    'close_price': close_price,
    'date': date
}
df = pd.DataFrame(data_df, index=[0])
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

# Load environment variables
load_dotenv()
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

# Create database connection
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

df.to_sql("crypto_prices", con=engine, if_exists="append", index=False, schema="dataengineering")
print(f"Successfully loaded crypto data for {df['date'][0]}")
