from get_data import get_stock_data
from clean_data import clean_stock_data
from visualize_data import visualize_stock_data
from load_to_sql import load_to_sql_server

def main():
    # Step 1: Get stock data
    symbol = 'AAPL'
    raw_data = get_stock_data(symbol)
    
    # Step 2: Clean the data
    cleaned_data = clean_stock_data(raw_data)
    
    # Step 3: Visualize the data
    visualize_stock_data(cleaned_data, symbol)
    
    # Step 4: Load data into SQL Server
    load_to_sql_server(cleaned_data, 'Stock_Prices')

if __name__ == "__main__":
    main()
