# historic_example.py
import pandas as pd
from auth_v2 import smartApi, logger

try:
    historicParams = {
        "exchange": "NSE",
        "symboltoken": "11536",
        "interval": "ONE_DAY",
        "fromdate": "2026-03-16 09:00", 
        "todate": "2026-03-24 15:30"
    }
    candles = smartApi.getCandleData(historicParams)
    #logger.info(candles)
    df = pd.DataFrame(candles["data"], columns=["datetime", "open", "high", "low", "close", "volume"])

    df.to_csv("candles.csv", index=False)
    print(df)

except Exception as e:
    logger.exception(f"Historic API failed: {e}")
