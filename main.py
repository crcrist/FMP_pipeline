import asyncio
from get_data import get_stock_list
from get_data import get_stock_data
from clean_data import clean_stock_data
from visualize_data import visualize_stock_data
from load_to_sql import load_to_sql_server

## possible addition in the future to process multiple stocks concurrently

# symbols = ['AAPL', 'MSFT', 'GOOGL']  # Example stock symbols
# await asyncio.gather(*(fetch_and_process_stock(symbol) for symbol in symbols))

async def fetch_and_process_stock(symbol):

    raw_data = await get_stock_data(symbol)

    cleaned_data = clean_stock_data(raw_data)

    await load_to_sql_server(cleaned_data, 'Stock_Prices')

async def main():
    # Fetch the stock list asynchronously
    stock_list = await get_stock_list()

    # Insert stock list data into SQL Server
    await load_to_sql_server(stock_list, 'Stock_List')

    # Pick a stock symbol to fetch historical data (or loop over many)
    symbols = ['AAPL', 'MSFT', 'GOOGL']  # Example stock symbols
    await asyncio.gather(*(fetch_and_process_stock(symbol) for symbol in symbols))
    
    # # Fetch and process historical data for a stock symbol, before integrating asynchrony 
    # await fetch_and_process_stock(symbol)

if __name__ == "__main__":
    # Run the main function asynchronously
    asyncio.run(main())
