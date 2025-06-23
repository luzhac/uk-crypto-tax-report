import math
from collections import deque

import pandas as pd


def calculate_pnl(trades: pd.DataFrame) -> pd.DataFrame:
    """
    FIFO PnL calculator.
    Calculates realised profit and commissions for short positions based on trade history.
    Parameters
    ----------
    trades : pd.DataFrame
        Expected columns:
            ['datetime', 'symbol', 'side', 'price',
             'qty', 'commission', 'commissionAsset', 'orderId']

    Returns
    -------
    pd.DataFrame
        Columns:
            ['open_datetime', 'close_datetime', 'shortOrLong', 'symbol', 'qty',
             'profit', 'commission_usdt', 'commission_bnb']
    """
    if trades.empty:
        return pd.DataFrame(columns=[
            "open_datetime", "close_datetime", "shortOrLong", "symbol", "qty",
            "profit", "commission_usdt", "commission_bnb"
        ])

    trades = trades.sort_values("datetime").reset_index(drop=True)

    # One FIFO queue per symbol to hold still-open sell orders.
    queues: dict[str, deque] = defaultdict(deque)
    results = []

    symbol = trades.iloc[0]['symbol']
    queue = queues[symbol]

    for i, row in trades.iterrows():
        #  OPEN SHORT
        if row.side == "sell":
            queue.append({
                "open_datetime": row.datetime,
                "price": row.price,
                "orig_qty": row.qty,
                "qty_left": row.qty,  # remaining quantity to be closed
                "commission": row.commission,
                "commissionAsset": row.commissionAsset,
                "profit": 0.0,  # accumulated realised PnL
                "commission_usdt": 0.0,
                "commission_bnb": 0.0,
            })

    # todo, if first row in startdate aroutn 0:00 to 1:00 check manul
    for i, row in trades.iterrows():
        if row.side == "buy":
            qty_to_close = row.qty
            comm_per_unit = row.commission / row.qty if row.qty else 0.0

            while qty_to_close:
                # to do what if closd  closing quantity than open shorts
                if not queue:
                    raise ValueError("More closing quantity than open shorts")

                opener = queue[0]  # oldest open short
                take = min(qty_to_close, opener["qty_left"])

                #  Realised PnL
                opener["profit"] += (opener["price"] - row.price) * take

                #  Allocate commissions proportionally to quantity matched
                ratio_open = take / opener["orig_qty"]
                if opener["commissionAsset"] == "USDT":
                    opener["commission_usdt"] += opener["commission"] * ratio_open
                elif opener["commissionAsset"] == "BNB":
                    opener["commission_bnb"] += opener["commission"] * ratio_open

                if row.commissionAsset == "USDT":
                    opener["commission_usdt"] += comm_per_unit * take
                elif row.commissionAsset == "BNB":
                    opener["commission_bnb"] += comm_per_unit * take

                # Update remaining quantities
                opener["qty_left"] -= take
                qty_to_close -= take
                last_close_time = row.datetime

                # If the sell is now fully closed, record a result and pop it.
                if opener["qty_left"] == 0:
                    results.append({
                        "open_datetime": opener["open_datetime"],
                        "close_datetime": last_close_time,
                        "shortOrLong": "short",
                        "symbol": row.symbol,
                        "qty": opener["orig_qty"],
                        "profit": opener["profit"],
                        "commission_usdt": round(opener["commission_usdt"], 10),
                        "commission_bnb": round(opener["commission_bnb"], 10),
                    })
                    queue.popleft()  # drop the completed position

    return pd.DataFrame(results)


def calculate_pnl_one(sell_orders, buy_orders):
    results = []
    for i in range(min(len(buy_orders), len(sell_orders))):
        sell_order = sell_orders[i]
        buy_order = buy_orders[i]
        if not math.isclose(sell_order['qty'], buy_order['qty'], rel_tol=1e-7, abs_tol=1e-7):
            raise ValueError('ValueError')

        qty = sell_order['qty'] + buy_order['qty']
        price_sell = sell_order['price']
        price_buy = buy_order['price']
        profit = (price_sell - price_buy) * sell_order['qty']
        commission_usdt = 0
        commission_bnb = 0
        if sell_order['commissionAsset'] == 'USDT':
            commission_usdt += sell_order['commission']
        if sell_order['commissionAsset'] == 'BNB':
            commission_bnb += sell_order['commission']
        if buy_order['commissionAsset'] == 'USDT':
            commission_usdt += buy_order['commission']
        if buy_order['commissionAsset'] == 'BNB':
            commission_bnb += buy_order['commission']

        direction = 'long'
        if sell_order['open_time'] < buy_order['open_time']:
            direction = 'short'

        # Calculate proceeds and cost
        proceeds = price_sell * sell_order['qty']
        cost = price_buy * buy_order['qty']
        gain_loss = proceeds - cost

        # Extract asset from symbol (assuming format like DATAUSDT)
        asset = sell_order['symbol'].replace('USDT', '')

        # Format disposal date
        disposal_date = max(sell_order['open_time'], buy_order['open_time'])

        # Format quantity to avoid long floating point numbers
        qty_formatted = round(qty, 4)
        if qty == int(qty):
            qty_formatted = int(qty)

        # Create concise notes with properly formatted numbers
        if direction == 'long':
            notes = f"Long {qty_formatted} {asset} @ {price_buy:.5f}, closed @ {price_sell:.5f}"
        else:
            notes = f"Short {qty_formatted} {asset} @ {price_sell:.5f}, closed @ {price_buy:.5f}"

        # Truncate notes if too long
        if len(notes) > 60:
            notes = notes[:57] + "..."


        results.append({
            "open_time": min(sell_order['open_time'], buy_order['open_time']),
            "close_time": max(sell_order['open_time'], buy_order['open_time']),
            "price_sell": price_sell,
            "price_buy": price_buy,
            "symbol": sell_order['symbol'],
            "direction": direction,
            "qty": qty,
            "profit": profit,
            "commission_usdt": commission_usdt,
            "commission_bnb": commission_bnb,
            # New fields for tax reporting
            "disposal_date": disposal_date,
            "asset": asset,
            "proceeds": proceeds,
            "cost": cost,
            "gain_loss": gain_loss,
            "notes": notes
        })

    result_summary = {
        "datetime": sell_orders[0]['open_time'] if len(sell_orders) > 0 else buy_orders[0]['open_time'],
        "symbol": sell_orders[0]['symbol'] if len(sell_orders) > 0 else buy_orders[0]['symbol'],
        "qty": sum(result["qty"] for result in results),
        "profit": sum(result["profit"] for result in results),
        "commission_usdt": sum(result["commission_usdt"] for result in results),
        "commission_bnb": sum(result["commission_bnb"] for result in results),
        # Summary tax fields
        "total_proceeds": sum(result["proceeds"] for result in results),
        "total_cost": sum(result["cost"] for result in results),
        "total_gain_loss": sum(result["gain_loss"] for result in results),
        "asset": sell_orders[0]['symbol'].replace('USDT', '') if len(sell_orders) > 0 else buy_orders[0][
            'symbol'].replace('USDT', ''),
        "disposal_date": max(result["disposal_date"] for result in results) if results else None,
    }

    return results, result_summary


def modify_trade_list(common_cum_qtys, trade_list):
    # modify open_shorts to match common_cum_qtys
    for common_cum_qty in common_cum_qtys:
        for i in range(len(trade_list)):
            cum_qty = trade_list[i]['cum_qty']
            if common_cum_qty == cum_qty:
                break
            if common_cum_qty < cum_qty:
                diff = cum_qty - common_cum_qty

                symbol = trade_list[i]['symbol']

                price = trade_list[i]['price']
                qty = trade_list[i]['qty']

                commission = trade_list[i]['commission']
                commissionAsset = trade_list[i]['commissionAsset']

                open_time = trade_list[i]['open_time']
                trade_list[i]['qty'] = qty - diff
                trade_list[i]['commission'] = commission * ((qty - diff) / qty)

                trade_list[i]['quoteQty'] = price * trade_list[i]['qty']
                trade_list[i]['cum_qty'] = cum_qty - diff

                trade_list.insert(i + 1,
                                  {
                                      "symbol": symbol,
                                      "open_time": open_time,
                                      "cum_qty": cum_qty,
                                      "qty": diff,
                                      "price": price,
                                      "quoteQty": price * diff,
                                      "commission": commission * ((diff) / qty),
                                      "commissionAsset": commissionAsset,
                                  })
                break


def add_cum_qty(trades):
    cum_qty = 0
    for i in range(len(trades)):
        cum_qty += trades[i]['qty']
        trades[i]['cum_qty'] = cum_qty
    return trades


def get_common_cum_qty(sell_orders, buy_orders):
    common_cum_qtys = []
    i, j = 0, 0

    while i < len(sell_orders) and j < len(buy_orders):
        if sell_orders[i]['cum_qty'] == buy_orders[j]['cum_qty']:
            common_cum_qtys.append(sell_orders[i]['cum_qty'])
            i += 1
            j += 1
        elif sell_orders[i]['cum_qty'] < buy_orders[j]['cum_qty']:
            common_cum_qtys.append(sell_orders[i]['cum_qty'])
            i += 1
        else:
            common_cum_qtys.append(buy_orders[j]['cum_qty'])
            j += 1

    return common_cum_qtys


def calculate_pnl_2(trades: pd.DataFrame):
    trades = trades.sort_values("datetime", kind="mergesort").reset_index(drop=True).copy()

    sell_orders = []
    buy_shorts = []

    for i, row in trades.iterrows():

        ts: pd.Timestamp = row["datetime"]

        symbol = row["symbol"]
        side = row["side"]
        price = float(row["price"])
        qty = float(row["qty"])
        quoteQty = float(row["quoteQty"])
        commission = float(row["commission"])
        commissionAsset = row["commissionAsset"]

        if side == "sell":  # open short
            sell_orders.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "price": price,
                    "commission": commission,
                    "quoteQty": quoteQty,
                    "commissionAsset": commissionAsset,
                    "open_time": pd.to_datetime(ts),
                }
            )

        if side == "buy":
            buy_shorts.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "price": price,
                    "commission": commission,
                    "quoteQty": quoteQty,
                    "commissionAsset": commissionAsset,
                    "open_time": pd.to_datetime(ts),
                }
            )

    sell_orders = add_cum_qty(sell_orders)
    buy_shorts = add_cum_qty(buy_shorts)
    common_cum_qty = get_common_cum_qty(sell_orders, buy_shorts)
    modify_trade_list(common_cum_qty, sell_orders)
    modify_trade_list(common_cum_qty, buy_shorts)
    results, summary_result = calculate_pnl_one(sell_orders, buy_shorts)
    return results, summary_result
    # print(summary_result)



def sum_interest():
    raw_folder = './data/raw/interest'
    os.makedirs(raw_folder, exist_ok=True)

    results = pd.DataFrame()


    for filename in os.listdir(raw_folder):
        if filename.endswith('.csv'):
            filepath = os.path.join(raw_folder, filename)
            trades = pd.read_csv(filepath)

            results=pd.concat(results,trades)


    # 把结果存为 DataFrame
    df = pd.DataFrame(results)
    summary=[]
    for asset in df['asset'].unique():
        df[ df['asset']==asset].sum('interest')
        summary.append({asset:df[ df['asset']==asset].sum('interest')})


