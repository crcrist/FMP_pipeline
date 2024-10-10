# import requests
import aiohttp # makes asynch http calls
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('FMP_API_KEY')

if not API_KEY:
    raise Exception("API Key not found. Ensure it's in the .env file.")

async def get_stock_list():
    url = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return pd.DataFrame(data)
            else:
                raise Exception(f"Error fetching stock list: {response.status}")

async def get_stock_data(symbol: str):
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return pd.DataFrame(data['historical'])
            else: 
                raise Exception(f"Error fetching data for {symbol}: {response.status}")

