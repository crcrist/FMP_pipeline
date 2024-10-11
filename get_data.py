# import requests
import aiohttp # makes asynch http calls
import aioodbc
import os
import pandas as pd
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv('FMP_API_KEY')

if not API_KEY:
    logger.error("API Key not found. Ensure it's in the .env file.")
    raise Exception("API Key not found. Ensure it's in the .env file.")

async def get_stock_symbols_from_db():
    try:
        connection_string = os.getenv('SQL_SERVER_CONNECTION_STRING')
        logger.info("Connecting to SQL Server to get stock symbols")

        async with aioodbc.connect(dsn=connection_string) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("Select symbol from Stock_List")
                rows = await cursor.fetchall()
                symbols = [row[0] for row in rows]
                logger.info(f"Fetched {len(symbols)} stock symbols from database")
                return symbols
    except Exception as e:
        logger.error(f"Error fetching stock symbols from DB: {e}", exc_info=True)
        raise

async def get_stock_list():
    try:
        logger.info("Fetching stock list from API")
        url = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}'

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info("Stock list successfully fetched from API")
                    return pd.DataFrame(data)
                else:
                    logger.error(f"Error fetching stock list: {response.status}")
                    raise Exception(f"Error fetching stock list: {response.status}")
    except Exception as e:
        logger.error(f"Error in get_stock_list: {e}", exc_info=True)
        raise

async def get_stock_data(symbol: str):
    try:
        logger.info(f"Fetching stock data for {symbol}")   
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={API_KEY}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully fetched stock data for {symbol}")
                    return pd.DataFrame(data['historical'])
                else: 
                    logger.error(f"Error fetching data for {symbol}: {response.status}")
                    raise Exception(f"Error fetching data for {symbol}: {response.status}")
    except Exception as e:
        logger.error(f"Error in get_stock_data for {symbol}: {e}", exc_info=True)
        raise

