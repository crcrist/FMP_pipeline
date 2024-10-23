import requests
import pyodbc
import logging
from datetime import datetime
import os 
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants 
API_KEY = os.getenv('FMP_API_KEY')
SQL_CONNECTION_STRING = os.getenv('SQL_SERVER_CONNECTION_STRING')
STOCK_LIST_URL = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"
# HISTORICAL_DATA_URL = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey=" + API_KEY
NASDAQ_EXCHANGE = "NASDAQ"

def get_db_connection():
    print({SQL_CONNECTION_STRING})
    return pyodbc.connect(SQL_CONNECTION_STRING)
    print("connected to sql server")

def fetch_stock_list():
    logger.info("Feching stock list from NASDAQ")
    response = requests.get(STOCK_LIST_URL)
    response.raise_for_status()
    return [stock for stock in response.json() if stock['exchange'] == NASDAQ_EXCHANGE]

def stock_exists(symbol, conn):
    query = "SELECT COUNT(*) FROM Stock_List WHERE Symbol = ?"
    cursor = conn.cursor()
    cursor.execute(query, (symbol,))
    count = cursor.fetchone()[0]
    return count > 0

def insert_stock(symbol, name, exchange, conn):
    logger.info(f"Inserting new stock symbol: {symbol}")
    insert_ts = datetime.now()
    query = "INSERT INTO Stock_List (symbol, name, exchange, insertTs) VALUES (?, ?, ?, ?)"
    cursor = conn.cursor()
    cursor.execute(query, (symbol, name, exchange, insert_ts))
    conn.commit()

def fetch_historical_data(symbol):
    logger.info(f"fetching historical data for symbol: {symbol}")
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("historical", [])

def insert_historical_data(symbol, historical_data, conn):
    logger.info(f"Inserting historical data for symbol: {symbol}")
    cursor = conn.cursor()
    for record in historical_data:
        query = """
            INSERT INTO Stock_Prices (symbol, [date], [open], [high], [low], [close], adjClose, volume, unadjustedVolume, [change], changePercent, vwap, label, changeOverTime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(query, (
        symbol,
        record['date'], record['open'], record['high'], record['low'], record['close'], record['adjClose'], 
            record['volume'], record['unadjustedVolume'], record['change'], record['changePercent'], 
            record['vwap'], record['label'], record['changeOverTime']
        ))
    conn.commit()

def main():
    conn = get_db_connection()

    symbols_to_process = ["AAPL", "MSFT", "NVDA"]

    try:
        stock_list = fetch_stock_list()
        for stock in stock_list:
            symbol, name, exchange = stock['symbol'], stock['name'], stock['exchange']
            if symbol in symbols_to_process:  # Only process stocks in the allowed list
                if not stock_exists(symbol, conn):
                    insert_stock(symbol, name, exchange, conn)
                else:
                    logger.info(f"Stock {symbol} already exists in the Stock_List table.")
            else:
                logger.info(f"Skipping stock {symbol} as it's not in the selected list.")
        
        for symbol in symbols_to_process:
            historical_data = fetch_historical_data(symbol)
            if historical_data:
                insert_historical_data(symbol, historical_data, conn)
            else: 
                logger.warning(f"No historical data found for symbol: {symbol}")
    
    except Exception as e:
        logger.error(f"Error in processing: {e}", exc_info=True)
    finally:
        conn.close()
if __name__ == "__main__":
    main()