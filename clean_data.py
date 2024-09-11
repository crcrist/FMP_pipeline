import pandas as pd

def clean_stock_data(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'open', 'close', 'volume', 'high', 'low']]
    return df.dropna()
