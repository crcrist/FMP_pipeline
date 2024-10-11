import asyncio
import logging
from get_data import get_stock_list, get_stock_data, get_stock_symbols_from_db
from clean_data import clean_stock_data
from load_to_sql import load_to_sql_server

logging.basicConfig(
    level=logging.INFO,  # Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
    handlers=[
        logging.FileHandler('app.log'),  # Log to a file named 'app.log'
        logging.StreamHandler()          # Also log to console
    ]
)

logger = logging.getLogger(__name__) 

## possible addition in the future to process multiple stocks concurrently

# symbols = ['AAPL', 'MSFT', 'GOOGL']  # Example stock symbols
# await asyncio.gather(*(fetch_and_process_stock(symbol) for symbol in symbols))

async def fetch_and_process_stock(symbol):
    try: 
        logger.info(f"Fetching and processing stock data for: {symbol}")

        raw_data = await get_stock_data(symbol)
        logger.debug(f"Cleaned data for {symbol}: {raw_data}")

        cleaned_data = clean_stock_data(raw_data)  # Assign cleaned data here
        logger.debug(f"Cleaned data for {symbol}: {cleaned_data}")

        await load_to_sql_server(cleaned_data, 'Stock_Price', symbol=symbol)
        logger.info(f"Successfully inserted data for {symbol} into Stock_Price")
    except Exception as e:
        logger.error(f"Error processing stock {symbol}: {e}", exc_info=True)

async def main():
    try:
        logger.info("Fetching stock list")
        stock_list = await get_stock_list()
        await load_to_sql_server(stock_list, 'Stock_List')
        logger.info("Stock list inserted into SQL Server")

        logger.info("Fetching stock symbols from database")
        stock_symbols = await get_stock_symbols_from_db()

        logger.info(f"Processing historical data for symbols: {stock_symbols}")
        await asyncio.gather(*(fetch_and_process_stock(symbol) for symbol in stock_symbols))
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())