"""
Fyers Historical Data Fetcher
Uses the official Fyers API v3 to fetch historical candlestick data.
"""

import os
import sys
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
import time
from order_sender import send as send_order


# Import connection utilities from fyers_connection
from fyers_connection import (
    APP_ID,
    APP_TYPE,
    get_stored_token,
    is_token_valid
)

# NSE Stocks List
NSE_STOCKS = [
    "NSE:OBEROIRLTY-EQ", "NSE:AXISBANK-EQ", "NSE:KAYNES-EQ", "NSE:TMPV-EQ", "NSE:360ONE-EQ",
    "NSE:ADANIGREEN-EQ", "NSE:IGL-EQ", "NSE:AMBER-EQ", "NSE:RBLBANK-EQ", "NSE:LUPIN-EQ",
    "NSE:HEROMOTOCO-EQ", "NSE:FORTIS-EQ", "NSE:APLAPOLLO-EQ", "NSE:IREDA-EQ", "NSE:HINDZINC-EQ",
    "NSE:YESBANK-EQ", "NSE:SAMMAANCAP-EQ", "NSE:NCC-EQ", "NSE:INDIGO-EQ", "NSE:IDEA-EQ",
    "NSE:CROMPTON-EQ", "NSE:POLYCAB-EQ", "NSE:SUPREMEIND-EQ", "NSE:PAGEIND-EQ", "NSE:BSE-EQ",
    "NSE:INDUSTOWER-EQ", "NSE:PNBHOUSING-EQ", "NSE:BANKINDIA-EQ", "NSE:GODREJPROP-EQ", "NSE:IRFC-EQ",
    "NSE:MPHASIS-EQ", "NSE:POWERGRID-EQ", "NSE:KALYANKJIL-EQ", "NSE:MAXHEALTH-EQ", "NSE:TECHM-EQ",
    "NSE:ADANIENT-EQ", "NSE:ETERNAL-EQ", "NSE:SAIL-EQ", "NSE:HDFCAMC-EQ", "NSE:MANKIND-EQ",
    "NSE:POLICYBZR-EQ", "NSE:CANBK-EQ", "NSE:HDFCBANK-EQ", "NSE:HUDCO-EQ", "NSE:JIOFIN-EQ",
    "NSE:AMBUJACEM-EQ", "NSE:CYIENT-EQ", "NSE:NYKAA-EQ", "NSE:DMART-EQ", "NSE:JINDALSTEL-EQ",
    "NSE:LODHA-EQ", "NSE:BHEL-EQ", "NSE:IDFCFIRSTB-EQ", "NSE:TRENT-EQ", "NSE:UNIONBANK-EQ",
    "NSE:VEDL-EQ", "NSE:IIFL-EQ", "NSE:LICHSGFIN-EQ", "NSE:INDUSINDBK-EQ", "NSE:OFSS-EQ",
    "NSE:TATAELXSI-EQ", "NSE:AUROPHARMA-EQ", "NSE:LTIM-EQ", "NSE:NAUKRI-EQ", "NSE:SRF-EQ",
    "NSE:SUZLON-EQ", "NSE:BAJAJ-AUTO-EQ", "NSE:CAMS-EQ", "NSE:CGPOWER-EQ", "NSE:DLF-EQ",
    "NSE:JSWSTEEL-EQ", "NSE:KPITTECH-EQ", "NSE:UNOMINDA-EQ", "NSE:RECLTD-EQ", "NSE:ADANIENSOL-EQ",
    "NSE:PAYTM-EQ", "NSE:TIINDIA-EQ", "NSE:KEI-EQ", "NSE:ABB-EQ", "NSE:BRITANNIA-EQ",
    "NSE:GRASIM-EQ", "NSE:BPCL-EQ", "NSE:HAL-EQ", "NSE:INDHOTEL-EQ", "NSE:MANAPPURAM-EQ",
    "NSE:PPLPHARMA-EQ", "NSE:TATATECH-EQ", "NSE:DRREDDY-EQ", "NSE:HCLTECH-EQ", "NSE:ITC-EQ",
    "NSE:NTPC-EQ", "NSE:PGEL-EQ", "NSE:TVSMOTOR-EQ", "NSE:BOSCHLTD-EQ", "NSE:BEL-EQ",
    "NSE:MCX-EQ", "NSE:PFC-EQ", "NSE:SBIN-EQ", "NSE:ADANIPORTS-EQ", "NSE:PETRONET-EQ",
    "NSE:INDIANB-EQ", "NSE:ASHOKLEY-EQ", "NSE:MARUTI-EQ", "NSE:PNB-EQ", "NSE:BAJFINANCE-EQ",
    "NSE:COALINDIA-EQ", "NSE:NATIONALUM-EQ", "NSE:HINDPETRO-EQ", "NSE:ICICIBANK-EQ", "NSE:LAURUSLABS-EQ",
    "NSE:SBICARD-EQ", "NSE:BANDHANBNK-EQ", "NSE:M&M-EQ", "NSE:DABUR-EQ", "NSE:KOTAKBANK-EQ",
    "NSE:SHREECEM-EQ", "NSE:CDSL-EQ", "NSE:DELHIVERY-EQ", "NSE:INOXWIND-EQ", "NSE:IOC-EQ",
    "NSE:MUTHOOTFIN-EQ", "NSE:GODREJCP-EQ", "NSE:NMDC-EQ", "NSE:TATAPOWER-EQ", "NSE:NBCC-EQ",
    "NSE:BHARATFORG-EQ", "NSE:HINDALCO-EQ", "NSE:MOTHERSON-EQ", "NSE:ONGC-EQ", "NSE:SBILIFE-EQ",
    "NSE:CONCOR-EQ", "NSE:NHPC-EQ", "NSE:COFORGE-EQ", "NSE:ASIANPAINT-EQ", "NSE:FEDERALBNK-EQ",
    "NSE:WIPRO-EQ", "NSE:CUMMINSIND-EQ", "NSE:PRESTIGE-EQ", "NSE:RELIANCE-EQ", "NSE:SYNGENE-EQ",
    "NSE:TORNTPHARM-EQ", "NSE:UPL-EQ", "NSE:COLPAL-EQ", "NSE:HFCL-EQ", "NSE:LT-EQ",
    "NSE:HAVELLS-EQ", "NSE:NUVAMA-EQ", "NSE:RVNL-EQ", "NSE:CIPLA-EQ", "NSE:KFINTECH-EQ",
    "NSE:BIOCON-EQ", "NSE:LICI-EQ", "NSE:ABCAPITAL-EQ", "NSE:BANKBARODA-EQ", "NSE:BDL-EQ",
    "NSE:BLUESTARCO-EQ", "NSE:DIVISLAB-EQ", "NSE:EXIDEIND-EQ", "NSE:GAIL-EQ", "NSE:ICICIPRULI-EQ",
    "NSE:JSWENERGY-EQ", "NSE:JUBLFOOD-EQ", "NSE:MAZDOCK-EQ", "NSE:NESTLEIND-EQ", "NSE:PATANJALI-EQ",
    "NSE:SOLARINDS-EQ", "NSE:SONACOMS-EQ", "NSE:TATACONSUM-EQ", "NSE:TCS-EQ", "NSE:TITAN-EQ",
    "NSE:VBL-EQ", "NSE:ZYDUSLIFE-EQ", "NSE:ALKEM-EQ", "NSE:IRCTC-EQ", "NSE:OIL-EQ",
    "NSE:POWERINDIA-EQ", "NSE:TORNTPOWER-EQ", "NSE:CHOLAFIN-EQ", "NSE:SIEMENS-EQ", "NSE:EICHERMOT-EQ",
    "NSE:INFY-EQ", "NSE:MARICO-EQ", "NSE:TITAGARH-EQ", "NSE:GLENMARK-EQ", "NSE:PERSISTENT-EQ",
    "NSE:SHRIRAMFIN-EQ", "NSE:ULTRACEMCO-EQ", "NSE:DALBHARAT-EQ", "NSE:HINDUNILVR-EQ", "NSE:PIDILITIND-EQ",
    "NSE:ASTRAL-EQ", "NSE:SUNPHARMA-EQ", "NSE:AUBANK-EQ", "NSE:PHOENIXLTD-EQ", "NSE:TATASTEEL-EQ",
    "NSE:APOLLOHOSP-EQ", "NSE:PIIND-EQ", "NSE:VOLTAS-EQ", "NSE:HDFCLIFE-EQ", "NSE:ANGELONE-EQ",
    "NSE:GMRAIRPORT-EQ", "NSE:BAJAJFINSV-EQ", "NSE:BHARTIARTL-EQ", "NSE:DIXON-EQ",
    "NSE:ICICIGI-EQ", "NSE:MFSL-EQ", "NSE:UNITDSPR-EQ", "NSE:LTF-EQ"
]


def cleanup_fyers_logs():
    """
    Remove Fyers API log files that are automatically created.
    These log files are not necessary for normal operation.
    """
    log_files = ['fyersApi.log', 'fyersRequests.log']
    for log_file in log_files:
        try:
            if os.path.exists(log_file):
                os.remove(log_file)
        except Exception:
            pass  # Silently ignore if file can't be deleted


def wait_for_market_time(target_hour=9, target_minute=10):
    """
    Wait until the specified market time (default 9:10 AM).
    This allows capturing pre-market opening data.
    
    Pre-market: 9:00 AM - 9:09 AM
    This function waits until 9:10 AM to start WebSocket
    
    Args:
        target_hour (int): Target hour (default: 9 for 9 AM)
        target_minute (int): Target minute (default: 10 for 9:10 AM)
    """
    now = datetime.now()
    target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    # If already past target time today, no need to wait
    if now >= target_time:
        print(f"✅ Current time {now.strftime('%H:%M:%S')} is past {target_hour:02d}:{target_minute:02d}")
        return
    
    # Calculate wait time
    wait_seconds = (target_time - now).total_seconds()
    
    print(f"\n⏰ Current time: {now.strftime('%H:%M:%S')}")
    print(f"⏰ Target time: {target_hour:02d}:{target_minute:02d}:00 (After pre-market)")
    print(f"⏰ Waiting for {wait_seconds/60:.1f} minutes...")
    print(f"\nPre-market runs 9:00-9:09 AM. Will connect at 9:10 AM to capture opening data.\n")
    
    # Wait with progress updates
    start_wait = datetime.now()
    while datetime.now() < target_time:
        elapsed = (datetime.now() - start_wait).total_seconds()
        remaining = wait_seconds - elapsed
        
        if remaining > 0:
            print(f"\r⏳ Waiting... {remaining/60:.1f} minutes remaining", end='', flush=True)
        
        time.sleep(10)  # Check every 10 seconds
    
    print(f"\n\n✅ Time reached: {datetime.now().strftime('%H:%M:%S')}")
    print("📊 Starting WebSocket connection now...\n")


def get_fyers_client():
    """
    Get an authenticated Fyers API client using stored token.
    
    Returns:
        fyersModel.FyersModel: Authenticated Fyers client
        
    Raises:
        Exception: If token is not found or invalid
    """
    token = get_stored_token()
    
    if not token:
        raise Exception("No stored token found. Please run fyers_connection.py first to authenticate.")
    
    if not is_token_valid(token):
        raise Exception("Stored token is invalid or expired. Please run fyers_connection.py to get a new token.")
    
    client_id = f"{APP_ID}-{APP_TYPE}"
    
    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=token,
        is_async=False,
        log_path=""
    )
    
    return fyers


def fetch_historical_data(symbol, resolution="D", range_from=None, range_to=None, 
                          date_format="0", cont_flag="1"):
    """
    Fetch historical candlestick data from Fyers API.
    
    Args:
        symbol (str): Trading symbol in Fyers format (e.g., "NSE:SBIN-EQ")
        resolution (str): Candle resolution. Options:
            - "1", "2", "3", "5", "10", "15", "20", "30", "60", "120", "240" for minutes
            - "D" for daily
            - "W" for weekly
            - "M" for monthly
        range_from (str/int): Start timestamp (Unix epoch) or date string
        range_to (str/int): End timestamp (Unix epoch) or date string
        date_format (str): 
            - "0" for Unix epoch timestamps in response
            - "1" for "yyyy-mm-dd" format in response
        cont_flag (str): 
            - "0" for only current expiry data
            - "1" for continuous data (for F&O)
    
    Returns:
        dict: API response containing candle data
            Format: {
                "candles": [[timestamp, open, high, low, close, volume], ...],
                "code": 200,
                "message": "",
                "s": "ok"
            }
            
    Raises:
        Exception: If API call fails or client cannot be initialized
    """
    # Get authenticated client
    fyers = get_fyers_client()
    
    # Set default date range if not provided (last 30 days)
    if range_from is None:
        range_from = int((datetime.now() - timedelta(days=30)).timestamp())
    if range_to is None:
        range_to = int(datetime.now().timestamp())
    
    # Convert datetime objects to timestamps if needed
    if isinstance(range_from, datetime):
        range_from = int(range_from.timestamp())
    if isinstance(range_to, datetime):
        range_to = int(range_to.timestamp())
    
    # Prepare the request data according to Fyers API documentation
    data = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": date_format,
        "range_from": str(range_from),
        "range_to": str(range_to),
        "cont_flag": cont_flag
    }
    
    # Fetch historical data using Fyers API
    response = fyers.history(data=data)
    
    return response


def fetch_intraday_data(symbol, resolution="5", days_back=1):
    """
    Convenience function to fetch intraday data for the last N days.
    
    Args:
        symbol (str): Trading symbol in Fyers format (e.g., "NSE:SBIN-EQ")
        resolution (str): Candle resolution in minutes ("1", "5", "15", "30", "60")
        days_back (int): Number of days to fetch data for (default: 1)
    
    Returns:
        dict: API response containing candle data
    """
    range_to = datetime.now()
    range_from = range_to - timedelta(days=days_back)
    
    return fetch_historical_data(
        symbol=symbol,
        resolution=resolution,
        range_from=range_from,
        range_to=range_to,
        date_format="0"
    )


def fetch_daily_data(symbol, days_back=365):
    """
    Convenience function to fetch daily data for the last N days.
    
    Args:
        symbol (str): Trading symbol in Fyers format (e.g., "NSE:SBIN-EQ")
        days_back (int): Number of days to fetch data for (default: 365)
    
    Returns:
        dict: API response containing candle data
    """
    range_to = datetime.now()
    range_from = range_to - timedelta(days=days_back)
    
    return fetch_historical_data(
        symbol=symbol,
        resolution="D",
        range_from=range_from,
        range_to=range_to,
        date_format="0"
    )


def parse_candles(response):
    """
    Parse the candle data from API response into a more usable format.
    
    Args:
        response (dict): API response from fetch_historical_data
        
    Returns:
        list: List of dictionaries with candle data
            [{"timestamp": ..., "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...}, ...]
        
    Raises:
        Exception: If response indicates an error
    """
    if response.get("s") != "ok":
        raise Exception(f"API Error: {response.get('message', 'Unknown error')} (Code: {response.get('code')})")
    
    candles = response.get("candles", [])
    parsed_data = []
    
    for candle in candles:
        parsed_data.append({
            "timestamp": candle[0],
            "datetime": datetime.fromtimestamp(candle[0]),
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4],
            "volume": candle[5]
        })
    
    return parsed_data


def get_last_trading_day():
    """
    Calculate the last trading day from today.
    If today is Monday, returns last Friday.
    Otherwise, returns the previous day.
    
    Returns:
        datetime: The last trading day
    """
    today = datetime.now()
    # Get weekday (0=Monday, 6=Sunday)
    weekday = today.weekday()
    
    if weekday == 0:  # Monday
        last_trading_day = today - timedelta(days=3)  # Last Friday
    elif weekday == 6:  # Sunday
        last_trading_day = today - timedelta(days=2)  # Last Friday
    else:
        last_trading_day = today - timedelta(days=1)  # Previous day
    
    return last_trading_day


def fetch_last_trading_day_all_stocks(save_to_csv=False, csv_filename="last_trading_day_data.csv"):
    """
    Fetch OHLCV data of the last trading day for all NSE stocks.
    
    Args:
        save_to_csv (bool): Whether to save the data to a CSV file (default: False)
        csv_filename (str): Name of the CSV file to save data (default: "last_trading_day_data.csv")
    
    Returns:
        pd.DataFrame: DataFrame containing OHLCV data for all stocks
            Columns: Symbol, Date, Open, High, Low, Close, Volume
    """
    print("=" * 80)
    print("Fetching OHLCV Data for Last Trading Day - All NSE Stocks")
    print("=" * 80)
    
    # Calculate last trading day
    last_trading_day = get_last_trading_day()
    last_trading_day_start = last_trading_day.replace(hour=0, minute=0, second=0, microsecond=0)
    last_trading_day_end = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"\n📅 Last Trading Day: {last_trading_day.strftime('%Y-%m-%d (%A)')}")
    print(f"📊 Total Stocks to Fetch: {len(NSE_STOCKS)}")
    print("\n" + "=" * 80)
    
    all_data = []
    successful_count = 0
    failed_count = 0
    failed_stocks = []
    
    for idx, symbol in enumerate(NSE_STOCKS, 1):
        try:
            print(f"\r[{idx}/{len(NSE_STOCKS)}] Fetching {symbol}...", end="", flush=True)
            
            # Fetch daily data for last trading day
            response = fetch_historical_data(
                symbol=symbol,
                resolution="D",
                range_from=last_trading_day_start,
                range_to=last_trading_day_end,
                date_format="0"
            )
            
            if response.get("s") == "ok" and response.get("candles"):
                candles = response.get("candles", [])
                
                # Get the last candle (most recent)
                if candles:
                    last_candle = candles[-1]
                    
                    all_data.append({
                        "Symbol": symbol,
                        "Date": datetime.fromtimestamp(last_candle[0]).strftime('%Y-%m-%d'),
                        "Timestamp": last_candle[0],
                        "Open": last_candle[1],
                        "High": last_candle[2],
                        "Low": last_candle[3],
                        "Close": last_candle[4],
                        "Volume": last_candle[5]
                    })
                    successful_count += 1
                else:
                    failed_count += 1
                    failed_stocks.append(symbol)
            else:
                failed_count += 1
                failed_stocks.append(symbol)
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            failed_stocks.append(symbol)
            # print(f"\n⚠️  Failed to fetch {symbol}: {str(e)}")
    
    print("\n" + "=" * 80)
    print("\n✅ Data Fetching Complete!")
    print(f"\n📊 Summary:")
    print(f"   • Total Stocks: {len(NSE_STOCKS)}")
    print(f"   • Successful: {successful_count}")
    print(f"   • Failed: {failed_count}")
    
    if failed_stocks:
        print(f"\n⚠️  Failed Stocks ({len(failed_stocks)}):")
        for stock in failed_stocks[:10]:  # Show first 10 failed stocks
            print(f"   • {stock}")
        if len(failed_stocks) > 10:
            print(f"   ... and {len(failed_stocks) - 10} more")
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        # Sort by symbol
        df = df.sort_values('Symbol').reset_index(drop=True)
        
        # Save to CSV if requested
        if save_to_csv:
            df.to_csv(csv_filename, index=False)
            print(f"\n💾 Data saved to: {csv_filename}")
        
        # Display sample data
        print("\n📈 Sample Data (First 5 Stocks):")
        print("-" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.head())
        print("-" * 80)
    else:
        print("\n❌ No data was fetched successfully.")
    
    print("\n" + "=" * 80)
    
    return df


# ============================================================================
# LIVE DATA STREAMING FUNCTIONALITY
# ============================================================================

# Global variables for WebSocket and filtering
live_data_buffer = []
message_count = 0
websocket_start_time = None
historical_close_prices = {}  # Store last trading day close prices by symbol
filtered_stocks = {}  # Store stocks that meet gap-up criteria
gap_up_checked = set()  # Track which stocks have been checked for gap-up
MAX_FILTERED_STOCKS = 2  # Maximum number of stocks to filter (top 2 with highest gap-up)
fyers_ws_instance = None  # Store WebSocket instance for closing connection


def onmessage(message):
    """
    Callback function to handle incoming WebSocket messages.
    Filters stocks with gap-up between 1.8% and 8.4%.
    
    Parameters:
        message (dict): The received message from the WebSocket containing live market data.
    """
    global message_count, live_data_buffer, historical_close_prices, filtered_stocks, gap_up_checked
    
    message_count += 1
    
    # Add timestamp
    message['received_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    # Store in buffer (keep last 1000)
    live_data_buffer.append(message)
    if len(live_data_buffer) > 1000:
        live_data_buffer.pop(0)
    
    # Check for gap-up filtering
    if 'symbol' in message:
        symbol = message.get('symbol', 'N/A')
        open_price = message.get('open_price')
        ltp = message.get('ltp', 'N/A')
        volume = message.get('vol_traded_today', 'N/A')
        change = message.get('ch', 'N/A')
        change_pct = message.get('chp', 'N/A')
        
        # Check for gap-up if we have historical data and haven't checked this stock yet
        if symbol in historical_close_prices and symbol not in gap_up_checked and open_price:
            prev_close = historical_close_prices[symbol]
            
            # Calculate gap-up percentage: (open - prev_close) / prev_close * 100
            gap_up_pct = ((open_price - prev_close) / prev_close) * 100
            
            # Filter: gap-up >= 1.8% and < 8.4% AND minimum stock price >= 100
            if 1.8 <= gap_up_pct < 8.4 and prev_close >= 100:
                # Add stock to filtered list
                filtered_stocks[symbol] = {
                    'prev_close': prev_close,
                    'open_price': open_price,
                    'gap_up_pct': gap_up_pct,
                    'ltp': ltp,
                    'timestamp': message['received_at']
                }
                
                # Keep only top N stocks with highest gap-up percentage
                if len(filtered_stocks) > MAX_FILTERED_STOCKS:
                    # Sort by gap_up_pct and keep only top N
                    sorted_stocks = sorted(
                        filtered_stocks.items(),
                        key=lambda x: x[1]['gap_up_pct'],
                        reverse=True
                    )
                    # Keep only top MAX_FILTERED_STOCKS
                    filtered_stocks.clear()
                    for sym, data in sorted_stocks[:MAX_FILTERED_STOCKS]:
                        filtered_stocks[sym] = data
                
                # Print alert for filtered stock
                print(f"\n\n🟢 CANDIDATE: {symbol} | Gap-Up: {gap_up_pct:.2f}% | Prev Close: {prev_close} | Open: {open_price}")
                
                # Show current top 2
                if filtered_stocks:
                    print(f"\n🏆 TOP {MAX_FILTERED_STOCKS} STOCKS (Updated):")
                    sorted_current = sorted(
                        filtered_stocks.items(),
                        key=lambda x: x[1]['gap_up_pct'],
                        reverse=True
                    )
                    for rank, (sym, data) in enumerate(sorted_current, 1):
                        print(f"   {rank}. {sym:20s} | Gap-Up: {data['gap_up_pct']:.2f}%")
                print("-" * 80)
            
            # Mark as checked
            gap_up_checked.add(symbol)
            
            # Check if all stocks have been evaluated
            if len(gap_up_checked) >= len(historical_close_prices):
                print("\n\n" + "=" * 80)
                print("✅ ALL STOCKS EVALUATED - SELECTION COMPLETE")
                print("=" * 80)
                
                if len(filtered_stocks) >= MAX_FILTERED_STOCKS:
                    print(f"\n🎯 Final Top {MAX_FILTERED_STOCKS} Shortlisted Stocks:")
                    sorted_final = sorted(
                        filtered_stocks.items(),
                        key=lambda x: x[1]['gap_up_pct'],
                        reverse=True
                    )
                    for rank, (sym, data) in enumerate(sorted_final, 1):
                        print(f"   {rank}. {sym:20s} | Gap-Up: {data['gap_up_pct']:.2f}% | Open: {data['open_price']:.2f}")
                
                print("\n🔌 Disconnecting WebSocket...")
                print("=" * 80 + "\n")
                
                # Unsubscribe and close the WebSocket connection
                if fyers_ws_instance:
                    try:
                        # Unsubscribe from all stocks
                        fyers_ws_instance.unsubscribe(symbols=NSE_STOCKS, data_type="SymbolUpdate")
                        print("✅ Unsubscribed from all symbols")
                    except Exception as e:
                        print(f"Note: Disconnect - {e}")
        
        # Print compact live update
        print(f"\r[{message_count:6d}] {symbol:20s} | LTP: {ltp:>8} | Vol: {volume:>10} | Chg: {change:>8} ({change_pct:>6}%)", 
              end='', flush=True)


def onerror(message):
    """
    Callback function to handle WebSocket errors.
    
    Parameters:
        message (dict): The error message from the WebSocket.
    """
    print(f"\n\n❌ WebSocket Error: {message}")


def onclose(message):
    """
    Modified: Now also sends FINAL top-2 stocks to Go TCP server.
    """
    global websocket_start_time, message_count, filtered_stocks

    print(f"\n\n🔌 WebSocket Connection Closed: {message}")

    # ---- SUMMARY BLOCK (same as before) ----
    if websocket_start_time:
        duration = (datetime.now() - websocket_start_time).total_seconds()
        print(f"\n📊 Live Data Session Summary:")
        print(f"   • Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        print(f"   • Messages Received: {message_count:,}")
        print(f"   • Avg Messages/sec: {message_count/duration:.2f}")
        print(f"   • Buffer Size: {len(live_data_buffer)}")

    print("\n" + "=" * 80)
    print(f"🎯 FINAL TOP {MAX_FILTERED_STOCKS} SELECTED STOCKS")
    print("=" * 80)

    if not filtered_stocks:
        print("\n⚠️ No stocks selected. Nothing will be sent to Go server.")
        return

    # ---- SORT FINAL WINNERS ----
    sorted_final = sorted(
        filtered_stocks.items(),
        key=lambda x: x[1]['gap_up_pct'],
        reverse=True
    )

    # ---- PRINT RESULTS ----
    for rank, (symbol, data) in enumerate(sorted_final, 1):
        print(f"{rank}. {symbol} | Gap-Up: {data['gap_up_pct']:.2f}% | Open: {data['open_price']}")

    print("\n" + "=" * 80)
    print("📤 SENDING FINAL 2 STOCKS TO GO TCP SERVER")
    print("=" * 80)

    # ---- SEND TO GO TCP SERVER ----
    for rank, (symbol, data) in enumerate(sorted_final[:2], 1):
        order_payload = {
            "rank": rank,
            "symbol": symbol,
            "exchange": "NSE",
            "qty": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "product": "MIS",
            "open_price": data["open_price"],
            "gap_up_pct": data["gap_up_pct"],
            "sent_at": datetime.now().isoformat()
        }

        try:
            send_order(order_payload)
            print(f"✅ SENT → {symbol} (Rank {rank})")
        except Exception as e:
            print(f"❌ FAILED to send {symbol}: {str(e)}")

    print("\n🚀 DONE — Go will now place orders exactly at 9:15\n")


def onopen(fyers_ws):
    """
    Callback function executed when WebSocket connection opens.
    Subscribes to all NSE stocks for live data.
    
    Parameters:
        fyers_ws: The Fyers WebSocket instance
    """
    global websocket_start_time, message_count, fyers_ws_instance
    
    # Store WebSocket instance for later disconnection
    fyers_ws_instance = fyers_ws
    
    print("\n\n✅ WebSocket Connection Established!")
    print("=" * 80)
    
    websocket_start_time = datetime.now()
    message_count = 0
    
    data_type = "SymbolUpdate"
    
    print(f"\n📡 Subscribing to {len(NSE_STOCKS)} NSE stocks for live updates...")
    print(f"📅 Start Time: {websocket_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔔 Data Type: {data_type}")
    print("\n" + "=" * 80)
    
    try:
        # Subscribe to all NSE stocks
        fyers_ws.subscribe(symbols=NSE_STOCKS, data_type=data_type)
        print(f"\n✅ Successfully subscribed to {len(NSE_STOCKS)} stocks!")
        print("\n📊 Streaming live data... (Press Ctrl+C to stop)\n")
        print("-" * 80)
        
        # Keep the socket running
        fyers_ws.keep_running()
        
    except Exception as e:
        print(f"\n❌ Subscription Error: {str(e)}")
        print("\n💡 Note: If you hit API limits, try reducing the number of stocks.")


def start_live_data_stream(historical_data=None, litemode=False):
    """
    Start live data streaming for all NSE stocks using WebSocket.
    Filters stocks with gap-up between 1.8% and 8.4%.
    
    Args:
        historical_data (pd.DataFrame): DataFrame containing last trading day data with 'Symbol' and 'Close' columns
        litemode (bool): Enable lite mode for reduced data response (default: False)
    
    Returns:
        None
    """
    global live_data_buffer, message_count, historical_close_prices, filtered_stocks, gap_up_checked
    
    print("\n" + "=" * 80)
    print("Starting Live Data Stream with Gap-Up Filtering")
    print("=" * 80)
    
    # Reset filtering variables
    filtered_stocks.clear()
    gap_up_checked.clear()
    
    # Store historical close prices
    if historical_data is not None and not historical_data.empty:
        for _, row in historical_data.iterrows():
            historical_close_prices[row['Symbol']] = row['Close']
        
        print(f"\n✅ Loaded historical data for {len(historical_close_prices)} stocks")
        print(f"🎯 Filtering criteria:")
        print(f"   • Gap-up >= 1.8% and < 8.4%")
        print(f"   • Minimum stock price >= ₹100")
        print(f"   • Select TOP {MAX_FILTERED_STOCKS} stocks with highest gap-up")
    else:
        print("\n⚠️  No historical data provided. Gap-up filtering will be skipped.")
    
    # Wait for 9:10 AM to capture pre-market opening data
    print("\n" + "=" * 80)
    print("⏰ WAITING FOR MARKET TIME")
    print("=" * 80)
    wait_for_market_time(target_hour=9, target_minute=10)
    print("=" * 80)
    
    try:
        # Get access token
        print("\n🔑 Authenticating...")
        token = get_stored_token()
        
        if not token:
            raise Exception("No stored token found. Please run fyers_connection.py first.")
        
        if not is_token_valid(token):
            raise Exception("Token is invalid or expired. Please run fyers_connection.py again.")
        
        access_token = f"{APP_ID}-{APP_TYPE}:{token}"
        print("✅ Authentication successful!")
        
        # Create WebSocket instance
        print("\n🔌 Connecting to Fyers WebSocket...")
        
        fyers_ws = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=litemode,
            write_to_file=False,
            reconnect=True,
            on_connect=lambda: onopen(fyers_ws),
            on_close=onclose,
            on_error=onerror,
            on_message=onmessage
        )
        
        # Connect to WebSocket
        fyers_ws.connect()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Stream interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\n❌ Error: {str(e)}")
        print("\n💡 Make sure you have a valid Fyers token.")
    finally:
        # Ask to save filtered stocks
        if filtered_stocks:
            print("\n" + "=" * 80)
            save_filtered = input("\n💾 Save filtered stocks to CSV? (y/n, default=y): ").strip().lower()
            if save_filtered != 'n':
                filename = input("Enter filename (default=filtered_stocks.csv): ").strip()
                if not filename:
                    filename = "filtered_stocks.csv"
                
                # Convert filtered stocks to DataFrame
                filtered_data = []
                for symbol, data in filtered_stocks.items():
                    filtered_data.append({
                        'Symbol': symbol,
                        'Prev_Close': data['prev_close'],
                        'Open': data['open_price'],
                        'Gap_Up_Pct': data['gap_up_pct'],
                        'LTP': data['ltp'],
                        'Timestamp': data['timestamp']
                    })
                
                df_filtered = pd.DataFrame(filtered_data)
                df_filtered = df_filtered.sort_values('Gap_Up_Pct', ascending=False)
                df_filtered.to_csv(filename, index=False)
                print(f"\n✅ Saved {len(df_filtered)} filtered stocks to: {filename}")
        
        # Ask to save buffer data
        if live_data_buffer:
            print("\n" + "=" * 80)
            save_choice = input("\n💾 Save live data buffer to CSV? (y/n, default=n): ").strip().lower()
            if save_choice == 'y':
                filename = input("Enter filename (default=live_data_buffer.csv): ").strip()
                if not filename:
                    filename = "live_data_buffer.csv"
                
                df = pd.DataFrame(live_data_buffer)
                df.to_csv(filename, index=False)
                print(f"\n✅ Saved {len(df)} records to: {filename}")



def main():
    """
    Automated Stock Filtering and Selection System.
    Fetches historical data, waits for 9:10 AM, streams live data, and selects top 2 stocks.
    """
    print("=" * 80)
    print("Fyers Automated Stock Selector")
    print("=" * 80)
    print("\n🎯 Workflow:")
    print("   1. Fetch last trading day OHLCV data")
    print("   2. Wait for 9:10 AM (after pre-market)")
    print("   3. Stream live data and filter stocks")
    print("   4. Select top 2 stocks with highest gap-up")
    print("   5. Auto-disconnect and save results")
    print("\n" + "=" * 80)
    
    try:
        # STEP 1: Fetch last trading day data for all stocks
        print("\n" + "=" * 80)
        print("STEP 1: Fetching Last Trading Day Data")
        print("=" * 80)
        df = fetch_last_trading_day_all_stocks()
        
        # STEP 2: Start live streaming with historical data for filtering
        print("\n" + "=" * 80)
        print("STEP 2: Live Data Streaming & Stock Selection")
        print("=" * 80)
        
        # Automatically start live streaming (no user prompt)
        start_live_data_stream(historical_data=df, litemode=False)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Make sure to run fyers_connection.py first to authenticate and save your token.")
    finally:
        # Cleanup Fyers log files
        cleanup_fyers_logs()



if __name__ == "__main__":
    main()
