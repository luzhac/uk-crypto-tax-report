import os
import pandas as pd
from dotenv import load_dotenv

from exchanges.binance import BinanceExchange
from src.data_processing import calculate_pnl_2
from src.report_generation import generate_uk_crypto_tax_pdf_report
from src.utils import  get_usd_to_gbp_from_yahoo
load_dotenv()


api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
start_time = '2024-04-06'
end_time = '2025-04-05'
exchange = BinanceExchange(api_key=api_key, api_secret=api_secret,start_time=start_time,end_time=end_time)

#trades = exchange.get_trades(symbol='BTCUSDT', start='2024-04-01', end='2024-04-30')
#interest = exchange.get_interest_history()



def calculate_pnl(market,usdt_to_gbp_df,bnb_to_usdt_df):
    # market is spot, margin, or future
    raw_folder = './data/raw/'+market
    os.makedirs(raw_folder, exist_ok=True)
    processed_folder = './data/processed/'+market
    os.makedirs(processed_folder, exist_ok=True)
    results = []
    results_summary = []

    for filename in os.listdir(raw_folder):
        if filename.endswith('.csv'):
            filepath = os.path.join(raw_folder, filename)
            trades = pd.read_csv(filepath)

            result, summary = calculate_pnl_2(trades)
            print(f"{filename}:\n{summary}\n")
            results.extend(result)
            results_summary.append(summary)

    
    df = pd.DataFrame(results)
    df['market']=market
    df['exchange'] = 'BINANCE'
    df_summary = pd.DataFrame(results_summary)
    df.sort_values('open_time',inplace=True)
    print('Summary:',df_summary['profit'].sum(),df_summary['commission_usdt'].sum(),df_summary['commission_bnb'].sum())

    output_path = os.path.join(processed_folder, 'combined_pnl.csv')
    df.to_csv(output_path, index=False)
    print(f"Saved to: {output_path}")

    df['open_time'] = pd.to_datetime(df['open_time'])
    df['open_time_minute'] = (df['open_time']).dt.floor('min')
    df['open_time_day'] = (df['open_time']).dt.floor('D')

    df = pd.merge_asof(
        df.sort_values('open_time_day'),
        usdt_to_gbp_df[['USD_to_GBP']].sort_index().rename(columns={'USD_to_GBP': 'usd_gbp'}),
        left_on='open_time_day',

        right_index=True,
        direction='backward'
    )

    df = pd.merge_asof(
        df.sort_values('open_time_minute'),
        bnb_to_usdt_df[['close']].sort_index().rename(columns={'close': 'bnb_usdt'}),
        left_on='open_time_minute',

        right_index=True,
        direction='backward'
    )


    df['proceeds_in_gbp'] = df['proceeds'] * df['usd_gbp']
    df['cost_in_gbp'] = df['cost'] * df['usd_gbp']
    df['profit_in_gbp'] = df['proceeds_in_gbp']-df['cost_in_gbp']
    df['commission_in_gbp'] = df['commission_usdt'] * df['usd_gbp'] + df['commission_bnb'] * \
                               df['bnb_usdt'] * df['usd_gbp']
    df['cost_in_gbp']= df['cost_in_gbp']+df['commission_in_gbp']
    df['net_profit_in_gbp'] = df['profit_in_gbp'] - df['commission_in_gbp']

    return df

 

def get_report():

    #exchange.get_price_minute('BNB','USDT')
    get_usd_to_gbp_from_yahoo(start = '2024-04-01',end=end_time)



    usdt_to_gbp_df=pd.read_csv('./data/usd_gbp.csv', index_col=0)
    # df[Price   ,     Date,  USD_to_GBP]
    usdt_to_gbp_df=usdt_to_gbp_df.iloc[1:]
    usdt_to_gbp_df['Date']=pd.to_datetime(usdt_to_gbp_df['Date'])
    usdt_to_gbp_df.set_index('Date')
    usdt_to_gbp_df=usdt_to_gbp_df.set_index('Date')
    usdt_to_gbp_df = usdt_to_gbp_df.sort_index()


    bnb_to_usdt_df=pd.read_csv('./data/bnb_usdt.csv', index_col=0)
    # df[datetime  close]
    bnb_to_usdt_df['datetime']=pd.to_datetime(bnb_to_usdt_df['datetime'])
    bnb_to_usdt_df=bnb_to_usdt_df.set_index('datetime')
    bnb_to_usdt_df = bnb_to_usdt_df.sort_index()


    trades_spot_df = calculate_pnl('spot', usdt_to_gbp_df, bnb_to_usdt_df)
    trades_margin_df = calculate_pnl('margin', usdt_to_gbp_df, bnb_to_usdt_df)
    # open_time,close_time,symbol,qty,profit,commission_usdt,commission_bnb
    df_combined = pd.concat([trades_spot_df, trades_margin_df], ignore_index=True)

    interest_df = pd.read_csv('./data/raw/interest/interest_margin.csv')
    interest_df['interestAccuredTime']=pd.to_datetime(interest_df['interestAccuredTime'])
    interest_df = pd.merge_asof(
        interest_df.sort_values('interestAccuredTime'),
        bnb_to_usdt_df[['close']].sort_index().rename(columns={'close': 'bnb_usdt'}),
        left_on='interestAccuredTime',
        right_index=True,
        direction='backward'
    )
    interest_df = pd.merge_asof(
        interest_df.sort_values('interestAccuredTime'),
        usdt_to_gbp_df[['USD_to_GBP']].sort_index().rename(columns={'USD_to_GBP': 'usd_to_gbp'}),
        left_on='interestAccuredTime',
        right_index=True,
        direction='backward'
    )
    interest_df['interest_in_usd'] = interest_df['interest'] * interest_df['bnb_usdt']
    interest_sum=(interest_df['interest_in_usd'].sum())

    print(trades_margin_df.tail())



    generate_uk_crypto_tax_pdf_report(df_combined,interest_sum)

#exchange.get_all_isolated_margin_interest_history_all_year()
#exchange.get_margin_interest_history_all_year()
get_report()
