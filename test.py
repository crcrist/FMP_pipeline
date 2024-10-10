# import requests
# import os
# import pandas as pd
# from dotenv import load_dotenv
# load_dotenv()

# API_KEY = os.getenv('FMP_API_KEY')



# url = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}'

# response = requests.get(url)

# if response.status_code == 200:
#     stocks = response.json()

#     df = pd.DataFrame(stocks)

#     # print(df.head)
#     walmart_stock = df.loc[df['symbol'] == 'WMT']

#     print(walmart_stock)
# else: 
#     print(f"Failed to fetch data. Status code: {response.status_code}")


