# instrument_master.py
import os
import json
import urllib.request
import pandas as pd
from datetime import datetime, timedelta

# Constants
SCRIPT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
KEY_PATH = "./data"  # adjust if needed
INSTRUMENT_CSV = os.path.join(KEY_PATH, "instrument_master.csv")

def load_instrument_master(cache_days: int = 1):
    """
    Load Angel Instrument Master.
    - Uses local CSV if < cache_days old
    - Otherwise downloads from Angel & caches
    Returns: list of dicts
    """
    try:
        os.makedirs(os.path.dirname(INSTRUMENT_CSV), exist_ok=True)

        # If cached CSV exists & is fresh
        if os.path.exists(INSTRUMENT_CSV):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(INSTRUMENT_CSV))
            if datetime.now() - file_mtime < timedelta(days=cache_days):
                print(f"Loaded Instrument Master from CSV (last updated: {file_mtime:%Y-%m-%d %H:%M:%S})")
                df = pd.read_csv(INSTRUMENT_CSV, dtype=str)
                return df.fillna("").to_dict("records")
            else:
                print("Instrument CSV is older than cache limit. Downloading fresh...")

        else:
            print("No cached Instrument Master found. Downloading...")

        # Download fresh JSON
        with urllib.request.urlopen(SCRIPT_MASTER_URL) as resp:
            instruments = json.loads(resp.read())

        # Save to CSV
        pd.DataFrame(instruments).to_csv(INSTRUMENT_CSV, index=False)
        print(f"Saved fresh Instrument Master to: {INSTRUMENT_CSV}")

        return instruments

    except Exception as e:
        print("Warning while preparing Instrument Master:", e)
        if os.path.exists(INSTRUMENT_CSV):
            print("Falling back to existing CSV cache.")
            df = pd.read_csv(INSTRUMENT_CSV, dtype=str)
            return df.fillna("").to_dict("records")
        raise


#file csv sample
'''
token	symbol	name	expiry	strike	lotsize	instrumenttype	exch_seg	tick_size
99926000	Nifty 50	NIFTY		0	1	AMXIDX	NSE	0
99926001	Nifty GrowSect 15	NIFTY GROWSECT 15		0	1	AMXIDX	NSE	0
99926002	Nifty50 PR 2x Lev	NIFTY50 PR 2X LEV		0	1	AMXIDX	NSE	0
99926004	Nifty 500	NIFTY 500		0	1	AMXIDX	NSE	0
99926008	Nifty IT	NIFTY IT		0	1	AMXIDX	NSE	0
99926009	Nifty Bank	BANKNIFTY		0	1	AMXIDX	NSE	0


'''

# Load instruments (cached or fresh)
instruments = load_instrument_master()
#save to csv
