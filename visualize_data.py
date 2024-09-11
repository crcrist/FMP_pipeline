import matplotlib.pyplot as plt
import pandas as pd

def visualize_stock_data(df: pd.DataFrame, stock_name: str):
    plt.figure(figsize=(10,6))
    plt.plot(df['date'], df['close'], label='Close Price', color='blue')
    plt.title(f'{stock_name} Stock Price History')
    plt.xlabel('Date')
    plt.ylabel('Close Price (USD)')
    plt.legend()
    plt.grid(True)
    plt.show()
