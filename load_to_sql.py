# import pyodbc
import aioodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

async def load_to_sql_server(df: pd.DataFrame, table_name: str):

    connection_string = os.getenv('SQL_SERVER_CONNECTION_STRING')

    async with aioodbc.connect(dsn=connection_string) as conn:
        async with conn.cursor() as cursor:
            for index, row in df.iterrows():
                price = row['price'] if 'price' in df.columns and pd.notnull(row['price']) else 0.0  # Ensure valid float
                if 'symbol' in df.columns:
                    print(f"Inserting row: {row}")
                    await cursor.execute(f"""
                        INSERT INTO {table_name} (symbol, exchange, exchangeShortName, price, name)
                        VALUES (?, ?, ?, ?, ?)
                    """, row['symbol'], row['exchange'], row['exchangeShortName'], price, row['name'])
                else:
                    await cursor.execute(f"""
                        INSERT INTO {table_name} (date, open_price, close_price, high, low, volume)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, row['date'], row['open'], row['close'], row['high'], row['low'], row['volume'])

        await conn.commit()
    # cursor.close()
    # # conn.close()
    # redundant because async with takes care of closing 
