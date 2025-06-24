import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from exchanges.base_exchange import BaseExchange
from binance.spot import Spot
from ratelimit import limits, sleep_and_retry

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


class BinanceExchange(BaseExchange):
    def __init__(self, api_key, api_secret, start_time, end_time):
        self.client = Spot(api_key=api_key, api_secret=api_secret)
        self.start_time = start_time
        self.end_time = end_time

    def get_trades(self, symbol, start, end):

        ...

    @sleep_and_retry
    @limits(calls=600, period=60)
    def get_price_minute(self, asset1, asset2):
        start_date = self.start_time
        end_date = self.end_time
        symbol = str(asset1).upper() + str(asset2).upper()
        interval = '1m'
        limit = 1000

        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000)

        all_klines = []

        while start_ts < end_ts:
            try:
                klines = self.client.klines(
                    symbol=symbol,
                    interval=interval,
                    startTime=start_ts,
                    endTime=min(start_ts + limit * 60_000, end_ts),
                    limit=limit
                )
                if not klines:
                    break
                all_klines.extend(klines)
                start_ts = klines[-1][0] + 60_000  # next minute
                print(pd.to_datetime(start_ts, unit='ms'))

            except Exception as e:
                print(f"Error: {e}")
                time.sleep(1)

        df = pd.DataFrame(all_klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        df['datetime'] = pd.to_datetime(df['open_time'], unit='ms')
        df = df[['datetime', 'close']]
        df['close'] = df['close'].astype(float)

        folder = './data'
        filename = asset1.lower() + '_' + asset2.lower() + '.csv'
        os.makedirs(folder, exist_ok=True)
        filepath = os.path.join(folder, filename)
        df.to_csv(filepath)

        return df

    def get_usdt_price_in_gbp(self, ts):

        ...

    def get_usdt_price_in_gbp(self, ts):

        ...

    def get_margin_trades(self, symbol, start_date, end_date, file_path):
        # api_key    = os.environ["BINANCE_API_KEY"]
        # api_secret = os.environ["BINANCE_SECRET_KEY"]

        all_trades = []
        current_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000) - 1

        while current_start < end:
            current_end = min(current_start + 86399999, end)
            start_ms = current_start
            end_ms = current_end

            from_id = None
            while True:
                params = {
                    'symbol': symbol,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': 500
                }
                if from_id:
                    params['fromId'] = from_id
                for i in range(5):
                    try:
                        trades = self.client.margin_my_trades(**params)
                        print(f"Fetching: {params}")
                        break
                    except Exception as e:
                        print(
                            f"Error fetching {datetime.fromtimestamp(start_ms / 1000)} – {datetime.fromtimestamp(end_ms / 1000)}: {e}")
                    time.sleep(3)

                all_trades.extend(trades)
                if len(trades) < 500:
                    break

                from_id = trades[-1]['id'] + 1
                time.sleep(0.3)

            print(f"{datetime.fromtimestamp(current_start / 1000).date()} fetched: {len(all_trades)} trades total")
            current_start = current_end + 1

        df = pd.DataFrame(all_trades)
        if df.empty:
            return df

        df['datetime'] = pd.to_datetime(df['time'], unit='ms')
        df['side'] = df['isBuyer'].map({True: 'buy', False: 'sell'})
        df['price'] = df['price'].astype(float)
        df['qty'] = df['qty'].astype(float)
        df['quoteQty'] = df['quoteQty'].astype(float)
        df['commission'] = df['commission'].astype(float)

        Path("./data/tax").mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)

        return df[
            ['datetime', 'symbol', 'side', 'price', 'qty', 'quoteQty', 'commission', 'commissionAsset', 'orderId']]

    def get_margin_trades(self, symbol, start_date, end_date, file_path):
        # api_key    = os.environ["BINANCE_API_KEY"]
        # api_secret = os.environ["BINANCE_SECRET_KEY"]

        all_trades = []
        current_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000) - 1

        while current_start < end:
            current_end = min(current_start + 86399999, end)
            start_ms = current_start
            end_ms = current_end

            from_id = None
            while True:
                params = {
                    'symbol': symbol,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': 500
                }
                if from_id:
                    params['fromId'] = from_id
                for i in range(5):
                    try:
                        trades = self.client.margin_my_trades(**params)
                        print(f"Fetching: {params}")
                        break
                    except Exception as e:
                        print(
                            f"Error fetching {datetime.fromtimestamp(start_ms / 1000)} – {datetime.fromtimestamp(end_ms / 1000)}: {e}")
                    time.sleep(3)

                all_trades.extend(trades)
                if len(trades) < 500:
                    break

                from_id = trades[-1]['id'] + 1
                time.sleep(0.3)

            print(f"{datetime.fromtimestamp(current_start / 1000).date()} fetched: {len(all_trades)} trades total")
            current_start = current_end + 1

        df = pd.DataFrame(all_trades)
        if df.empty:
            return df

        df['datetime'] = pd.to_datetime(df['time'], unit='ms')
        df['side'] = df['isBuyer'].map({True: 'buy', False: 'sell'})
        df['price'] = df['price'].astype(float)
        df['qty'] = df['qty'].astype(float)
        df['quoteQty'] = df['quoteQty'].astype(float)
        df['commission'] = df['commission'].astype(float)

        Path("./data/tax").mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, index=False)

        return df[
            ['datetime', 'symbol', 'side', 'price', 'qty', 'quoteQty', 'commission', 'commissionAsset', 'orderId']]

    @sleep_and_retry
    @limits(calls=60, period=60)
    def get_spot_trades(self, symbol, start_date, end_date, file_path):

        all_trades = []
        current_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end = int((datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000) - 1

        while current_start < end:
            current_end = min(current_start + 86399999, end)
            start_ms = current_start
            end_ms = current_end

            from_id = None
            while True:
                params = {
                    'symbol': symbol,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': 500
                }
                if from_id:
                    params['fromId'] = from_id

                for i in range(5):  # Retry up to 5 times
                    try:
                        trades = self.client.my_trades(**params)
                        print(f"Fetching: {params}")
                        break
                    except Exception as e:
                        print(
                            f"Error fetching {datetime.fromtimestamp(start_ms / 1000)} – {datetime.fromtimestamp(end_ms / 1000)}: {e}")
                        time.sleep(3)
                else:
                    print("Failed after 5 retries.")
                    break

                if not trades:
                    break

                all_trades.extend(trades)
                if len(trades) < 500:
                    break

                from_id = trades[-1]['id'] + 1
                time.sleep(0.3)

            print(f"{datetime.fromtimestamp(current_start / 1000).date()} fetched: {len(all_trades)} trades total")
            current_start = current_end + 1

        if not all_trades:
            print("No trades found.")
            return pd.DataFrame()

        df = pd.DataFrame(all_trades)

        df['datetime'] = pd.to_datetime(df['time'], unit='ms')
        df['side'] = df['isBuyer'].map({True: 'buy', False: 'sell'})
        df['price'] = df['price'].astype(float)
        df['qty'] = df['qty'].astype(float)
        df['quoteQty'] = df['quoteQty'].astype(float)
        df['commission'] = df['commission'].astype(float)
        df['symbol'] = symbol  # Add symbol for completeness

        df.to_csv(file_path, index=False)
        print(f"Saved to: {os.path.abspath(file_path)}")

        return df[
            ['datetime', 'symbol', 'side', 'price', 'qty', 'quoteQty', 'commission', 'commissionAsset', 'orderId']]

    def get_spot_records(self, symbols):
        # print(get_available_usdt_symbols())

        symbols = [symbol for symbol in symbols if symbol >= 'ACAUSDT']
        start_date, end_date = '2024-04-06', '2025-04-05'  ##In the tax year 6 April 2024 to 5 April 2025:
        for symbol in symbols:
            if symbol.endswith('USDT'):
                print(symbol)
                file_path = Path(f"./analyse/data/tax/spot/{symbol}_margin_trades.csv")

                if file_path.exists():
                    continue


                else:
                    self.get_spot_trades(symbol, start_date, end_date, file_path)

    def get_trade_records(self, symbols):
        # print(get_available_usdt_symbols())
        symbols = ['CTXCUSDT']
        start_date, end_date = '2024-04-06', '2025-04-05'  ##In the tax year 6 April 2024 to 5 April 2025:
        for symbol in symbols:
            if symbol.endswith('USDT'):
                print(symbol)
                file_path = Path(f"./data/tax/margin/{symbol}_margin_trades.csv")

                if file_path.exists():
                    continue


                else:
                    self.get_margin_trades(symbol, start_date, end_date, file_path)


    def get_margin_interest_history_all_year(self, isolatedSymbol=None):
        start_date_str = self.start_time
        end_date_str = self.end_time

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        all_records = []
        current_start = start_date

        while current_start < end_date:

            current_end = min(current_start + timedelta(days=30), end_date)
            print(f"Fetching {current_start.date()} to {current_end.date()}")

            # Implement pagination for each time window
            page = 1
            while True:
                try:
                    response = self.client.margin_interest_history(
                        isolatedSymbol=isolatedSymbol,
                        startTime=int(current_start.timestamp() * 1000),
                        endTime=int(current_end.timestamp() * 1000),
                        current=page,
                        size=100
                    )

                    if not response.get('rows'):
                        break

                    all_records.extend(response['rows'])

                    # Check if we've got all records for this period
                    if len(response['rows']) < 100:
                        break

                    page += 1
                    time.sleep(0.1)  # Small delay between pagination requests

                except Exception as e:
                    print(f"Error from {current_start.date()} to {current_end.date()}, page {page}: {e}")
                    time.sleep(2)  # Wait before continuing
                    break

            # Move to next period without gaps
            current_start = current_end
            time.sleep(0.5)

            df = pd.DataFrame(all_records)

            if not df.empty:
                # Add validation
                print(f"Total interest records fetched: {len(df)}")
                if 'interestAccuredTime' in df.columns:
                    df['interestAccuredTime'] = pd.to_datetime(df['interestAccuredTime'], unit='ms')
                    print(f"Date range: {df['interestAccuredTime'].min()} to {df['interestAccuredTime'].max()}")

                    # Check for date gaps
                    df_sorted = df.sort_values('interestAccuredTime')
                    date_gaps = df_sorted['interestAccuredTime'].diff().dt.days
                    large_gaps = date_gaps[date_gaps > 2]  # Gaps larger than 2 days
                    if not large_gaps.empty:
                        print(f"Warning: Found {len(large_gaps)} potential date gaps in interest data")

                df['interestAccuredTime'] = pd.to_datetime(df['interestAccuredTime'], unit='ms')

                raw_folder = './data/raw/interest'
                if isolatedSymbol:
                    filename = 'interest_margin_' + isolatedSymbol + '.csv'
                else:
                    filename = 'interest_margin.csv'
                os.makedirs(raw_folder, exist_ok=True)
                filepath = os.path.join(raw_folder, filename)
                df.to_csv(filepath)
            return df

    def get_available_spot_usdt_symbols(self):
        BASE_URL = "https://data.binance.vision/?prefix=data/spot/daily/trades/"
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
        symbols = []

        try:
            driver.get(BASE_URL)
            time.sleep(20)  # Allow time for links to load

            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute("href")
                if href and href.endswith("USDT/"):
                    symbol = href.strip("/").split("/")[-1]
                    symbols.append(symbol)

            return symbols

        finally:
            driver.quit()

    def get_all_isolated_margin_interest_history_all_year(self):
        symbols = self.get_available_spot_usdt_symbols()
        for symbol in symbols:
            print(f'fetch  {symbol}')
            self.get_margin_interest_history_all_year(symbol)



