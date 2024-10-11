import aioodbc
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

async def load_to_sql_server(df: pd.DataFrame, table_name: str, symbol: str = None):
    try:
        connection_string = os.getenv('SQL_SERVER_CONNECTION_STRING')
        logger.info(f"Connecting to SQL Server to load data into {table_name}")

        async with aioodbc.connect(dsn=connection_string) as conn:
            async with conn.cursor() as cursor:
                for index, row in df.iterrows():
                    insert_time = datetime.now() 
                    if table_name == 'Stock_List':
                        logger.debug(f"Inserting row into Stock_list: {row}")
                        await cursor.execute(f"""
                            INSERT INTO {table_name} (symbol, exchange, exchangeShortName, name, insert_time)
                            VALUES (?, ?, ?, ?, ?)
                        """, row['symbol'], row['exchange'], row['exchangeShortName'], row['name'], insert_time)
                    elif table_name == 'Stock_Price':
                        logger.debug(f"Inserting historical data for {symbol}: {row}")
                        await cursor.execute(f"""
                            INSERT INTO {table_name} (date, open_price, close_price, high, low, volume, insert_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, row['date'], row['open'], row['close'], row['high'], row['low'], row['volume'], insert_time)
        
            await conn.commit()
        logger.info(f"Data successfully inserted into {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}", exc_info=True)
        raise
    # cursor.close()
    # # conn.close()
    # redundant because async with takes care of closing 
