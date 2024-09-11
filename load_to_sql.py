import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os

def load_to_sql_server(df: pd.DataFrame, table_name: str):

    connection_string = os.getenv('SQL_SERVER_CONNECTION_STRING')

    conn = pyodbc.connect(connection_string)


    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(f"INSERT INTO {table_name} (date, open_price, close_price, high, low, volume) VALUES (?, ?, ?, ?, ?, ?)", 
                       row['date'], row['open'], row['close'], row['high'], row['low'], row['volume'])

    conn.commit()
    cursor.close()
    conn.close()