import pandas as pd

from main import calculate_pnl

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
interest_df['interest_in_usd'] = interest_df['interest'] * interest_df['bnb_usdt']
interest_df['interest_in_gbp'] = interest_df['interest_in_usd'] * interest_df['usd_to_gbp']
df_combined['disposal_date'] = pd.to_datetime(df_combined['disposal_date'])
trades_sorted = df_combined[['disposal_date']].reset_index().rename(columns={'index': 'trade_id'}).sort_values(
    'disposal_date')
interest_sorted = interest_df.sort_values('interestAccuredTime')
merged = pd.merge_asof(
    interest_sorted,
    trades_sorted,
    left_on='interestAccuredTime',
    right_on='disposal_date',
    direction='nearest'
)
assigned = merged.groupby('trade_id')['interest_in_gbp'].sum().reset_index()
df_combined = df_combined.reset_index().rename(columns={'index': 'trade_id'})
df_combined = df_combined.merge(assigned, on='trade_id', how='left').fillna({'interest_in_gbp': 0})
df_combined['cost_in_gbp'] += df_combined['interest_in_gbp']
df_combined['net_profit_in_gbp'] -= df_combined['interest_in_gbp']
df_combined = df_combined.drop(columns=['trade_id'])

print(trades_margin_df.tail())