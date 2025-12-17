import os
import pandas as pd
from dotenv import load_dotenv

from exchanges.binance import BinanceExchange
from src.data_processing import calculate_pnl_2
from src.report_generation import generate_uk_crypto_tax_pdf_report
from src.utils import  get_usd_to_gbp_from_yahoo
from decimal import Decimal
load_dotenv()


api_key = 'ZrumLpueBNcG1LYnRCHxacWNkuD7qXgTOa8NoIaCLQjQ2tGUHRad043Ea0uIHGUl'#os.getenv("API_KEY")
api_secret = 'FhHnLIdaB9S9p9xaThFIqoF7CyENqNbfDgs3JjQVWptJhy88FIGDA3kMZZrAva9L'#os.getenv("API_SECRET")
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
    print(df.head())
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


    df['proceeds'] = df['proceeds'].apply(Decimal)
    df['usd_gbp'] = df['usd_gbp'].apply(Decimal)

    df['cost'] = df['cost'].apply(Decimal)
    df['usd_gbp'] = df['usd_gbp'].apply(Decimal)

    df['commission_usdt'] = df['commission_usdt'].apply(Decimal)
    df['commission_bnb'] = df['commission_bnb'].apply(Decimal)
    df['bnb_usdt'] = df['bnb_usdt'].apply(Decimal)







    df['proceeds_in_gbp'] = df['proceeds'] * df['usd_gbp']
    df['proceeds_in_gbp'] = df['proceeds_in_gbp'].apply(Decimal)

    df['cost_in_gbp'] = df['cost'] * df['usd_gbp']
    df['cost_in_gbp'] = df['cost_in_gbp'].apply(Decimal)

    df['profit_in_gbp'] = df['proceeds_in_gbp']-df['cost_in_gbp']
    df['profit_in_gbp'] = df['profit_in_gbp'].apply(Decimal)

    df['commission_in_gbp'] = df['commission_usdt'] * df['usd_gbp'] + df['commission_bnb'] * \
                               df['bnb_usdt'] * df['usd_gbp']
    df['commission_in_gbp'] = df['commission_in_gbp'].apply(Decimal)

    df['cost_in_gbp']= df['cost_in_gbp']+df['commission_in_gbp']
    df['cost_in_gbp'] = df['cost_in_gbp'].apply(Decimal)

    df['net_profit_in_gbp'] = df['profit_in_gbp'] - df['commission_in_gbp']
    df['net_profit_in_gbp'] = df['net_profit_in_gbp'].apply(Decimal)

    return df

 

def get_report():

    #exchange.get_price_minute('BNB','USDT')
    get_usd_to_gbp_from_yahoo(start = '2024-04-01',end=end_time)

    usdt_to_gbp_df = pd.read_csv('./data/usd_gbp.csv', index_col=0)
    # df[Price   ,     Date,  USD_to_GBP]
    usdt_to_gbp_df = usdt_to_gbp_df.iloc[1:]
    usdt_to_gbp_df['Date'] = pd.to_datetime(usdt_to_gbp_df['Date'])
    usdt_to_gbp_df.set_index('Date')
    usdt_to_gbp_df = usdt_to_gbp_df.set_index('Date')
    usdt_to_gbp_df = usdt_to_gbp_df.sort_index()

    bnb_to_usdt_df = pd.read_csv('./data/bnb_usdt.csv', index_col=0)
    # df[datetime  close]
    bnb_to_usdt_df['datetime'] = pd.to_datetime(bnb_to_usdt_df['datetime'])
    bnb_to_usdt_df = bnb_to_usdt_df.set_index('datetime')
    bnb_to_usdt_df = bnb_to_usdt_df.sort_index()

    trades_spot_df = calculate_pnl('spot', usdt_to_gbp_df, bnb_to_usdt_df)
    trades_margin_df = calculate_pnl('margin', usdt_to_gbp_df, bnb_to_usdt_df)
    # open_time,close_time,symbol,qty,profit,commission_usdt,commission_bnb
    df_combined = pd.concat([trades_spot_df, trades_margin_df], ignore_index=True)

    interest_df = pd.read_csv('./data/raw/interest/interest_margin.csv')
    interest_df['interestAccuredTime'] = pd.to_datetime(interest_df['interestAccuredTime'])
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

    interest_df['interest'] = interest_df['interest'].apply(Decimal)
    interest_df['bnb_usdt'] = interest_df['bnb_usdt'].apply(Decimal)

    interest_df['usd_to_gbp'] = interest_df['usd_to_gbp'].apply(Decimal)
    interest_df['usd_to_gbp'] = interest_df['usd_to_gbp'].apply(Decimal)

    interest_df['interest_in_usd'] = interest_df['interest'] * interest_df['bnb_usdt']
    interest_df['interest_in_usd'] = interest_df['interest_in_usd'].apply(Decimal)

    interest_df['interest_in_gbp'] = interest_df['interest_in_usd'] * interest_df['usd_to_gbp']
    interest_df['interest_in_gbp'] = interest_df['interest_in_gbp'].apply(Decimal)

    df_combined['disposal_date'] = pd.to_datetime(df_combined['disposal_date'])

    # add interest charge to trade cost, use data only from margin as interest only in margin
    trades_sorted = df_combined[df_combined['market'] == 'margin'][['disposal_date']].reset_index().rename(
        columns={'index': 'trade_id'}).sort_values('disposal_date')
    interest_sorted = interest_df.sort_values('interestAccuredTime')
    merged = pd.merge_asof(
        interest_sorted,
        trades_sorted,
        left_on='interestAccuredTime',
        right_on='disposal_date',
        direction='nearest'
    )
    merged['interest_in_gbp'] = merged['interest_in_gbp'].apply(Decimal)
    assigned = merged.groupby('trade_id')['interest_in_gbp'].sum().reset_index()
    df_combined = df_combined.reset_index().rename(columns={'index': 'trade_id'})
    df_combined = df_combined.merge(assigned, on='trade_id', how='left').fillna({'interest_in_gbp': 0})
    df_combined['interest_in_gbp'] = df_combined['interest_in_gbp'].apply(Decimal)
    df_combined['cost_in_gbp'] = df_combined['cost_in_gbp'].apply(Decimal)
    df_combined['cost_in_gbp'] += df_combined['interest_in_gbp']
    df_combined['cost_in_gbp'] = df_combined['cost_in_gbp'].apply(Decimal)

    df_combined['proceeds_in_gbp'] = df_combined['proceeds_in_gbp'].apply(Decimal)
    df_combined['net_profit_in_gbp'] = df_combined['proceeds_in_gbp']-df_combined['cost_in_gbp']
    df_combined['net_profit_in_gbp'] = df_combined['net_profit_in_gbp'].apply(Decimal)
    df_combined = df_combined.drop(columns=['trade_id'])

    print(trades_margin_df.tail())

    df_combined.to_csv('combined.csv')

    generate_uk_crypto_tax_pdf_report(df_combined)

def get_report1():
    df_combined=pd.read_csv('combined.csv')

    generate_uk_crypto_tax_pdf_report(df_combined)
#exchange.get_all_isolated_margin_interest_history_all_year()
#exchange.get_margin_interest_history_all_year()
#get_report()
get_report1()
