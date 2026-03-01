import pandas as pd
from exchanges.binance import BinanceExchange


import random
import time
from datetime import timedelta
from typing import List, Dict

def make_fake_margin_interest_payload(
    *,
    asset: str = "BNB",
    raw_asset: str = "BTTC",
    principal: float | str = 0.3,
    interest: str = "0.00000001",
    start_ts_ms: int | None = None,
    end_ts_ms: int | None = None,
    current: int = 1,
    size: int = 100,
) -> Dict[str, List[Dict]]:
    """
    Shape a dict exactly like Binance's response for
    GET /sapi/v1/margin/interestHistory.
    """
    now_ms = int(time.time() // 3600 * 3600 * 1_000)          # top of hour
    if end_ts_ms is None:
        end_ts_ms = now_ms
    else:
        end_ts_ms=end_ts_ms+60*60*24*1000-1
    if start_ts_ms is None:
        start_ts_ms = end_ts_ms - int(timedelta(days=1).total_seconds() * 1_000)


    rows_all: List[Dict] = []
    i=0
    while True:
        ts = end_ts_ms - i * 60 * 60 * 1_000  # step back 1 h per row
        i+=1
        if ts < start_ts_ms:
            break
        rows_all.append(
            {
                "txId": random.randint(10**18, 2 * 10**18 - 1),
                "interestAccuredTime": ts,
                "asset": asset,
                "rawAsset": raw_asset,
                "principal": str(principal),
                "interest": interest,
                "interestRate": f"{random.uniform(0.00050, 0.00060):.8f}",
                "type": "PERIODIC_CONVERTED",
            }
        )

    size = max(1, min(size, 100))
    start_idx = max(current, 1) - 1
    start_idx *= size
    end_idx = start_idx + size
    rows_page = rows_all[start_idx:end_idx]

    return {"total": len(rows_all), "rows": rows_page}

class StubSpot:
    """
    Only the `margin_interest_history` method is implemented, just enough to
    exercise pagination and basic time-window handling in unit tests.
    """
    def __init__(self):
        pass

    def margin_interest_history(self, *_, **kwargs):
        """
        Fake implementation of
        GET /sapi/v1/margin/interestHistory
        """
        return make_fake_margin_interest_payload(

            asset=kwargs.get("asset", "BNB"),
            start_ts_ms=kwargs.get("startTime"),
            end_ts_ms=kwargs.get("endTime"),
            current=int(kwargs.get("current", 1)),
            size=int(kwargs.get("size", 10)),
        )

def test_margin_interest_pagination():
    ex = BinanceExchange(
        api_key="X", api_secret="Y",
        start_time="2024-04-06", end_time="2024-05-05"
    )
    # Inject the fake client
    ex.client = StubSpot()
    df = ex.get_margin_interest_history_all_year()
    # Assertions
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 720