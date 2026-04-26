import os
import json
import time
import urllib.request
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from auth_v2 import smartApi, logger

# Constants from instrument_master.py
SCRIPT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
KEY_PATH = "./data"
INSTRUMENT_CSV = os.path.join(KEY_PATH, "instrument_master.csv")

# Configuration
STRIKE_INTERVAL = 50  # NIFTY strike interval
ITM_STRIKES = 5       # Number of ITM strikes to fetch
OTM_STRIKES = 5       # Number of OTM strikes to fetch
DEFAULT_TIMEFRAME = "ONE_MINUTE"
DEFAULT_DAYS = 5      # Default historical data days

class NiftyOptionsDownloader:
    def __init__(self):
        self.instruments = []
        self.spot_price = None
        self.atm_strike = None
        self.selected_expiry = None
        
    def load_instrument_master(self, cache_days: int = 1) -> List[Dict]:
        """Load Angel Instrument Master with caching"""
        try:
            os.makedirs(os.path.dirname(INSTRUMENT_CSV), exist_ok=True)

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

    def get_nifty_spot_price(self) -> float:
        """Get current NIFTY spot price using proven method"""
        try:
            # Use the proven method from options_filter.py
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)

            historicParams = {
                "exchange": "NSE",
                "symboltoken": "99926000",  # NIFTY index token
                "interval": "ONE_DAY",
                "fromdate": from_date.strftime("%Y-%m-%d 09:00"),
                "todate": to_date.strftime("%Y-%m-%d 15:30")
            }

            print("Fetching NIFTY spot price...")
            candles = self.get_candles_with_retry(historicParams)

            if candles["status"] and "data" in candles and candles["data"]:
                df = pd.DataFrame(
                    candles["data"],
                    columns=["datetime", "open", "high", "low", "close", "volume"]
                )
                
                # Convert datetime properly
                df["datetime"] = pd.to_datetime(df["datetime"])
                
                # Sort to be safe
                df = df.sort_values("datetime")
                
                # Get last available close
                latest_row = df.iloc[-1]
                latest_close = latest_row["close"]
                latest_date = latest_row["datetime"]
                
                print(f"Latest NIFTY Close: {latest_close} (Date: {latest_date})")
                return float(latest_close)
            else:
                print(f"Failed to fetch NIFTY spot price: {candles}")
                raise Exception("Failed to fetch NIFTY spot price")
                
        except Exception as e:
            print(f"Error fetching spot price: {e}")
            # Fallback to approximate spot price (you can adjust this)
            fallback_price = 19800.0
            print(f"Using fallback spot price: {fallback_price}")
            return fallback_price

    def calculate_atm_strike(self, spot_price: float) -> int:
        """Calculate ATM strike based on spot price"""
        atm = round(spot_price / STRIKE_INTERVAL) * STRIKE_INTERVAL
        print(f"Calculated ATM strike: {atm}")
        return atm

    def filter_nifty_options(self) -> List[Dict]:
        """Filter NIFTY options from instrument master using proven approach"""
        try:
            # Load and process data like options_filter.py
            df = pd.read_csv(INSTRUMENT_CSV, dtype=str)
            
            # Convert strike (paise to actual)
            df["strike"] = pd.to_numeric(df["strike"]) / 100
            
            # Convert expiry
            df["expiry"] = pd.to_datetime(df["expiry"], format="%d%b%Y", errors="coerce")
            
            # Filter NIFTY options (OPTIDX type)
            df = df[
                (df["name"] == "NIFTY") &
                (df["instrumenttype"] == "OPTIDX")
            ]
            
            # Get current expiry (minimum future expiry)
            current_date = datetime.now()
            future_expries = df[df["expiry"] >= current_date]
            
            if len(future_expries) == 0:
                print("No future expiries found, using earliest expiry")
                current_expiry = df["expiry"].min()
            else:
                current_expiry = future_expries["expiry"].min()
            
            # Filter for current expiry
            df = df[df["expiry"] == current_expiry]
            
            # Convert back to list of dicts
            nifty_options = df.fillna("").to_dict("records")
            
            print(f"Found {len(nifty_options)} NIFTY option contracts for expiry {current_expiry.strftime('%d%b%Y')}")
            
            # Store the selected expiry for later use
            self.selected_expiry = current_expiry.strftime('%d%b%Y')
            
            return nifty_options
            
        except Exception as e:
            print(f"Error filtering NIFTY options: {e}")
            return []

    def get_nearest_expiry(self, options: List[Dict]) -> str:
        """Find the nearest expiry date from available options"""
        current_date = datetime.now()
        nearest_expiry = None
        min_days_diff = float('inf')
        
        expiry_dates = set()
        for option in options:
            expiry_str = option.get('expiry', '')
            if expiry_str:
                try:
                    # Handle DDMMMYYYY format (e.g., "30MAR2026")
                    expiry_date = datetime.strptime(expiry_str, '%d%b%Y')
                    expiry_dates.add(expiry_date)
                except ValueError:
                    try:
                        # Try alternative format with dashes
                        expiry_date = datetime.strptime(expiry_str, '%d-%b-%Y')
                        expiry_dates.add(expiry_date)
                    except ValueError:
                        print(f"Warning: Could not parse expiry date: {expiry_str}")
                        continue
        
        print(f"Found {len(expiry_dates)} unique expiry dates")
        for expiry_date in sorted(expiry_dates):
            print(f"  {expiry_date.strftime('%d%b%Y')} ({expiry_date.strftime('%Y-%m-%d')})")
        
        # Find nearest future expiry
        for expiry_date in sorted(expiry_dates):
            if expiry_date >= current_date:
                days_diff = (expiry_date - current_date).days
                if days_diff < min_days_diff:
                    min_days_diff = days_diff
                    nearest_expiry = expiry_date
        
        if nearest_expiry:
            expiry_str = nearest_expiry.strftime('%d%b%Y')
            print(f"Selected nearest expiry: {expiry_str} ({nearest_expiry.strftime('%Y-%m-%d')})")
            return expiry_str
        
        raise Exception("No valid expiry dates found")

    def select_strikes_by_moneyness(self, atm_strike: int) -> Dict[str, List[int]]:
        """Select strikes based on moneyness"""
        strikes = {
            'ATM': [atm_strike],
            'ITM': [],
            'OTM': []
        }
        
        # ITM strikes (lower for calls, higher for puts - but we'll use same range for both)
        for i in range(1, ITM_STRIKES + 1):
            itm_strike = atm_strike - (i * STRIKE_INTERVAL)
            strikes['ITM'].append(itm_strike)
        
        # OTM strikes (higher for calls, lower for puts - but we'll use same range for both)
        for i in range(1, OTM_STRIKES + 1):
            otm_strike = atm_strike + (i * STRIKE_INTERVAL)
            strikes['OTM'].append(otm_strike)
        
        print("Selected strikes:")
        for moneyness, strike_list in strikes.items():
            print(f"  {moneyness}: {strike_list}")
        
        return strikes

    def filter_contracts_by_strikes_and_expiry(self, options: List[Dict], 
                                            strikes: Dict[str, List[int]], 
                                            expiry: str) -> Dict[str, Dict[str, List[Dict]]]:
        """Filter contracts by strikes and expiry"""
        selected_contracts = {
            'CE': {'ATM': [], 'ITM': [], 'OTM': []},
            'PE': {'ATM': [], 'ITM': [], 'OTM': []}
        }
        
        all_strikes = strikes['ATM'] + strikes['ITM'] + strikes['OTM']
        
        for option in options:
            # Check if expiry matches (already filtered, but double-check)
            option_expiry = option.get('expiry', '')
            if isinstance(option_expiry, pd.Timestamp):
                option_expiry_str = option_expiry.strftime('%d%b%Y')
            else:
                option_expiry_str = str(option_expiry)
            
            if option_expiry_str != expiry:
                continue
                
            # Get strike and identify option type from symbol
            strike_val = option.get('strike', '')
            symbol = option.get('symbol', '').upper()
            
            # Identify option type from symbol ending
            if symbol.endswith('CE'):
                instrument_type = 'CE'
            elif symbol.endswith('PE'):
                instrument_type = 'PE'
            else:
                continue
            
            try:
                # Strike is already converted to actual value in filtering
                strike = int(float(strike_val))
                
                if strike in all_strikes:
                    # Categorize by moneyness
                    if strike in strikes['ATM']:
                        moneyness = 'ATM'
                    elif strike in strikes['ITM']:
                        moneyness = 'ITM'
                    elif strike in strikes['OTM']:
                        moneyness = 'OTM'
                    else:
                        continue
                    
                    selected_contracts[instrument_type][moneyness].append(option)
                    print(f"Found {instrument_type} {moneyness} contract: strike {strike}, token {option.get('token')}")
                    
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not process strike {strike_val}: {e}")
                continue
        
        # Print summary
        for opt_type in ['CE', 'PE']:
            print(f"\n{opt_type} contracts found:")
            for moneyness in ['ATM', 'ITM', 'OTM']:
                count = len(selected_contracts[opt_type][moneyness])
                print(f"  {moneyness}: {count} contracts")
        
        return selected_contracts

    def get_candles_with_retry(self, params: Dict[str, Any], 
                              max_attempts: int = 5, 
                              base_delay: float = 1.0) -> Dict[str, Any]:
        """Retry wrapper for historic API calls"""
        last_resp: Dict[str, Any] = {"status": False, "message": "No call made"}
        
        for attempt in range(1, max_attempts + 1):
            try:
                resp = smartApi.getCandleData(params)
                
                if resp.get("status"):
                    return resp
                
                err_code = str(resp.get("errorcode", ""))
                msg = str(resp.get("message", ""))
                logger.warning(f"Historic API attempt {attempt}/{max_attempts} failed: errorcode={err_code}, message={msg}")
                last_resp = resp
                
                if err_code == "AB1004" or "Something Went Wrong" in msg:
                    sleep_s = base_delay * (2 ** (attempt - 1))
                    time.sleep(min(sleep_s, 8))
                    continue
                else:
                    break
                    
            except Exception as ex:
                err_str = str(ex)
                if "exceeding access rate" in err_str:
                    logger.warning(f"Rate limited on attempt {attempt}/{max_attempts}. Retrying...")
                else:
                    logger.error(f"Historic API exception on attempt {attempt}/{max_attempts}: {ex}")
                last_resp = {"status": False, "message": err_str}
                # Longer backoff for rate limits
                sleep_time = base_delay * (2 ** attempt)
                time.sleep(min(sleep_time, 15))
        
        return last_resp

    def download_historical_data(self, contract: Dict, timeframe: str = DEFAULT_TIMEFRAME, 
                              days: int = DEFAULT_DAYS) -> Optional[pd.DataFrame]:
        """Download historical data for a contract"""
        try:
            token = contract.get('token', '')
            symbol = contract.get('symbol', '')
            strike = contract.get('strike', '')
            instrument_type = contract.get('instrumenttype', '')
            
            if not token:
                print(f"No token found for contract: {symbol} {strike} {instrument_type}")
                return None
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            params = {
                "exchange": "NFO",
                "symboltoken": token,
                "interval": timeframe,
                "fromdate": start_date.strftime("%Y-%m-%d 09:15"),
                "todate": end_date.strftime("%Y-%m-%d %H:%M")
            }
            
            print(f"Downloading data for {symbol} {strike} {instrument_type}...")
            
            candles = self.get_candles_with_retry(params)
            
            if candles["status"] and "data" in candles and candles["data"]:
                df = pd.DataFrame(
                    candles["data"],
                    columns=["datetime", "open", "high", "low", "close", "volume"]
                )
                df["datetime"] = pd.to_datetime(df["datetime"])
                
                # Add missing metadata for backtesting analysis
                df["symbol"] = symbol
                df["strike"] = strike
                df["expiry_date"] = contract.get("expiry", "")
                df["instrument_type"] = instrument_type
                
                print(f"Downloaded {len(df)} records")
                return df
            else:
                print(f"No data returned for {symbol} {strike} {instrument_type}: {candles}")
                return None
                
        except Exception as e:
            print(f"Error downloading data for contract: {e}")
            return None

    def save_data_to_file(self, df: pd.DataFrame, contract: Dict, 
                         moneyness: str, expiry: str) -> None:
        """Save data to organized file structure"""
        try:
            # Determine opt_type (CE/PE)
            symbol = contract.get('symbol', '').upper()
            opt_type = 'CE' if symbol.endswith('CE') else 'PE'
            
            # Create directory structure
            base_dir = os.path.join(KEY_PATH, "NIFTY", f"expiry_{expiry}")
            opt_type_dir = os.path.join(base_dir, opt_type)
            os.makedirs(opt_type_dir, exist_ok=True)
            
            # Generate filename
            strike = contract.get('strike', '')
            filename = f"{opt_type}_{moneyness}_{strike}.csv"
            filepath = os.path.join(opt_type_dir, filename)
            
            # Save to CSV
            df.to_csv(filepath, index=False)
            print(f"Saved data to: {filepath}")
            
        except Exception as e:
            print(f"Error saving data: {e}")

    def run_downloader(self, timeframe: str = DEFAULT_TIMEFRAME, 
                      days: int = DEFAULT_DAYS) -> None:
        """Main execution method"""
        try:
            print("=== NIFTY Options Data Downloader ===")
            
            # Step 1: Load instrument master
            print("\n1. Loading instrument master...")
            self.instruments = self.load_instrument_master()
            
            # Step 2: Get spot price and calculate ATM
            print("\n2. Getting spot price and calculating ATM...")
            self.spot_price = self.get_nifty_spot_price()
            self.atm_strike = self.calculate_atm_strike(self.spot_price)
            
            # Step 3: Filter NIFTY options (this also gets current expiry)
            print("\n3. Filtering NIFTY options and selecting expiry...")
            nifty_options = self.filter_nifty_options()
            
            if not nifty_options:
                print("No NIFTY options found. Exiting.")
                return
            
            # Step 4: Select strikes by moneyness
            print("\n4. Selecting strikes by moneyness...")
            strikes = self.select_strikes_by_moneyness(self.atm_strike)
            
            # Step 5: Filter contracts by strikes
            print("\n5. Filtering contracts by strikes...")
            selected_contracts = self.filter_contracts_by_strikes_and_expiry(
                nifty_options, strikes, self.selected_expiry
            )
            
            # Step 6: Download data
            print("\n6. Downloading historical data...")
            total_downloaded = 0
            
            for opt_type in ['CE', 'PE']:
                for moneyness in ['ATM', 'ITM', 'OTM']:
                    contracts = selected_contracts[opt_type][moneyness]
                    
                    for i, contract in enumerate(contracts):
                        print(f"\nProcessing {opt_type} {moneyness} contract {i+1}/{len(contracts)}")
                        
                        df = self.download_historical_data(contract, timeframe, days)
                        if df is not None and not df.empty:
                            df["moneyness"] = moneyness
                            self.save_data_to_file(df, contract, moneyness, self.selected_expiry)
                            total_downloaded += 1
                        else:
                            print(f"Skipping contract - no data available")
                        
                        # Add a larger delay to ensure we don't hit the strict API limits
                        time.sleep(1.0)
            
            print(f"\n=== Download Complete ===")
            print(f"Total contracts downloaded: {total_downloaded}")
            print(f"Data saved to: {os.path.join(KEY_PATH, 'NIFTY')}")
            
        except Exception as e:
            print(f"Error in downloader: {e}")
            logger.exception(f"Downloader failed: {e}")

# Main execution
if __name__ == "__main__":
    downloader = NiftyOptionsDownloader()
    
    # You can customize these parameters
    custom_timeframe = "ONE_MINUTE"  # Options: ONE_MINUTE, FIVE_MINUTE, TEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, ONE_DAY
    custom_days = 5                  # Number of days of historical data
    
    downloader.run_downloader(timeframe=custom_timeframe, days=custom_days)
