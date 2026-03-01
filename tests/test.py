import os

from dotenv import load_dotenv

from exchanges.binance import BinanceExchange
load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
start_time = '2025-04-06'
end_time = '2026-04-05'

be=BinanceExchange(api_key=api_key, api_secret=api_secret,start_time=start_time,end_time=end_time)

be.get_future_download_link( )