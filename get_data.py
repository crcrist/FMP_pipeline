import requests
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('FMP_API_KEY')

if not API_KEY:
    raise Exception("API Key not found. Ensure it's in the .env file.")

def get_stock_data(symbol: str):
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['historical'])
    else:
        raise Exception(f"Error fetching data for {symbol}: {response.status_code}")
