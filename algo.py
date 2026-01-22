"""
Complete Algorithmic Trading System
- Runs automatically at 9:00 AM
- Step 1: Fyers Authentication (always required for data)
- Step 2: Stock Selection (Fyers WebSocket)
- Step 3: Order Execution at 9:15 AM (Fyers/Zerodha based on mode)
"""

import os
import sys
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
import time
import threading

# Import Fyers connection utilities
from fyers_connection import (
    APP_ID,
    APP_TYPE,
    get_stored_token,
    is_token_valid
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Execution Mode: "FYERS" or "ZERODHA"
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "FYERS")

# Stock Selection Parameters
MAX_FILTERED_STOCKS = 2
GAP_UP_MIN = 1.8
GAP_UP_MAX = 8.4
MIN_STOCK_PRICE = 100

# Order Execution Time
ORDER_EXECUTION_HOUR = 9
ORDER_EXECUTION_MINUTE = 15

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
# ============================================================================
# GLOBAL STATE - LOCAL VARIABLES (IN MEMORY)
# ============================================================================

# WebSocket state
live_data_buffer = []
message_count = 0
websocket_start_time = None
fyers_ws_instance = None

# Stock selection state
historical_close_prices = {}
filtered_stocks = {}
gap_up_checked = set()

# FINAL PAYLOAD - STORED IN LOCAL VARIABLE (NOT RAM/DISK)
selected_stocks_payload = [
    {
    "symbol":"NSE:SBIN-EQ",
    "qty":100,
    "type":2,
    "side":-1,
    "productType":"INTRADAY",    
},
    {
    "symbol":"NSE:TCS-EQ",
    "qty":100,
    "type":2,
    "side":-1,
    "productType":"INTRADAY",
}
]


# Fyers client (reused across functions)
fyers_client = None


# ============================================================================
# STEP 1: FYERS AUTHENTICATION
# ============================================================================

def authenticate_fyers():
    """
    Step 1: Authenticate with Fyers API
    Always required for stock data (regardless of execution mode)
    
    Returns:
        fyersModel.FyersModel: Authenticated Fyers client
    """
    global fyers_client
    
    print("\n" + "=" * 80)
    print("STEP 1: FYERS AUTHENTICATION")
    print("=" * 80)
    
    print("\n🔑 Authenticating with Fyers API...")
    
    try:
        # Get stored token
        token = get_stored_token()
        
        if not token:
            raise Exception(
                "❌ No Fyers token found!\n"
                "   Please run: python fyers_connection.py"
            )
        
        # Validate token
        if not is_token_valid(token):
            raise Exception(
                "❌ Fyers token expired!\n"
                "   Please run: python fyers_connection.py"
            )
        
        # Create Fyers client
        client_id = f"{APP_ID}-{APP_TYPE}"
        
        fyers_client = fyersModel.FyersModel(
            client_id=client_id,
            token=token,
            is_async=False,
            log_path=""
        )
        
        print(f"✅ Fyers authentication successful!")
        print(f"   Client ID: {client_id}")
        print(f"   Token: {token[:20]}...")
        
        return fyers_client
        
    except Exception as e:
        print(f"\n❌ Fyers authentication failed: {str(e)}")
        raise


# ============================================================================
# STEP 2: STOCK SELECTION (FYERS DATA)
# ============================================================================

def cleanup_fyers_logs():
    """Remove Fyers API log files."""
    log_files = ['fyersApi.log', 'fyersRequests.log']
    for log_file in log_files:
        try:
            if os.path.exists(log_file):
                os.remove(log_file)
        except Exception:
            pass


def wait_for_market_time(target_hour=9, target_minute=10):
    """Wait until the specified market time."""
    now = datetime.now()
    target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    if now >= target_time:
        print(f"✅ Current time {now.strftime('%H:%M:%S')} is past {target_hour:02d}:{target_minute:02d}")
        return
    
    wait_seconds = (target_time - now).total_seconds()
    
    print(f"\n⏰ Current time: {now.strftime('%H:%M:%S')}")
    print(f"⏰ Target time: {target_hour:02d}:{target_minute:02d}:00")
    print(f"⏰ Waiting for {wait_seconds/60:.1f} minutes...\n")
    
    start_wait = datetime.now()
    while datetime.now() < target_time:
        elapsed = (datetime.now() - start_wait).total_seconds()
        remaining = wait_seconds - elapsed
        
        if remaining > 0:
            print(f"\r⏳ Waiting... {remaining/60:.1f} minutes remaining", end='', flush=True)
        
        time.sleep(10)
    
    print(f"\n✅ Time reached: {datetime.now().strftime('%H:%M:%S')}\n")


def get_last_trading_day():
    """Calculate the last trading day."""
    today = datetime.now()
    weekday = today.weekday()
    
    if weekday == 0:  # Monday
        last_trading_day = today - timedelta(days=3)
    elif weekday == 6:  # Sunday
        last_trading_day = today - timedelta(days=2)
    else:
        last_trading_day = today - timedelta(days=1)
    
    return last_trading_day


def fetch_historical_data(symbol, resolution="D", range_from=None, range_to=None):
    """Fetch historical candlestick data from Fyers API."""
    global fyers_client
    
    if range_from is None:
        range_from = int((datetime.now() - timedelta(days=30)).timestamp())
    if range_to is None:
        range_to = int(datetime.now().timestamp())
    
    if isinstance(range_from, datetime):
        range_from = int(range_from.timestamp())
    if isinstance(range_to, datetime):
        range_to = int(range_to.timestamp())
    
    data = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "0",
        "range_from": str(range_from),
        "range_to": str(range_to),
        "cont_flag": "1"
    }
    
    response = fyers_client.history(data=data)
    return response


def fetch_last_trading_day_all_stocks():
    """Fetch OHLCV data of the last trading day for all NSE stocks."""
    print("\n" + "=" * 80)
    print("📊 Fetching Historical Data (Last Trading Day)")
    print("=" * 80)
    
    last_trading_day = get_last_trading_day()
    last_trading_day_start = last_trading_day.replace(hour=0, minute=0, second=0, microsecond=0)
    last_trading_day_end = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"\n📅 Last Trading Day: {last_trading_day.strftime('%Y-%m-%d (%A)')}")
    print(f"📊 Total Stocks: {len(NSE_STOCKS)}")
    print("\n" + "=" * 80)
    
    all_data = []
    successful_count = 0
    failed_count = 0
    
    for idx, symbol in enumerate(NSE_STOCKS, 1):
        try:
            print(f"\r[{idx}/{len(NSE_STOCKS)}] Fetching {symbol}...", end="", flush=True)
            
            response = fetch_historical_data(
                symbol=symbol,
                resolution="D",
                range_from=last_trading_day_start,
                range_to=last_trading_day_end
            )
            
            if response.get("s") == "ok" and response.get("candles"):
                candles = response.get("candles", [])
                
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
            else:
                failed_count += 1
            
            time.sleep(0.1)
            
        except Exception:
            failed_count += 1
    
    print(f"\n\n✅ Success: {successful_count} | ❌ Failed: {failed_count}")
    print("=" * 80 + "\n")
    
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        df = df.sort_values('Symbol').reset_index(drop=True)
    
    return df


def convert_fyers_to_zerodha_symbol(fyers_symbol):
    """Convert Fyers symbol to Zerodha format."""
    symbol = fyers_symbol.replace("NSE:", "").replace("-EQ", "")
    return symbol


def onmessage(message):
    """Handle incoming WebSocket messages and filter stocks."""
    global message_count, live_data_buffer, historical_close_prices, filtered_stocks, gap_up_checked
    
    message_count += 1
    message['received_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    live_data_buffer.append(message)
    if len(live_data_buffer) > 1000:
        live_data_buffer.pop(0)
    
    if 'symbol' in message:
        symbol = message.get('symbol', 'N/A')
        open_price = message.get('open_price')
        ltp = message.get('ltp', 'N/A')
        volume = message.get('vol_traded_today', 'N/A')
        
        if symbol in historical_close_prices and symbol not in gap_up_checked and open_price:
            prev_close = historical_close_prices[symbol]
            gap_up_pct = ((open_price - prev_close) / prev_close) * 100
            
            if (GAP_UP_MIN <= gap_up_pct < GAP_UP_MAX and prev_close >= MIN_STOCK_PRICE):
                filtered_stocks[symbol] = {
                    'prev_close': prev_close,
                    'open_price': open_price,
                    'gap_up_pct': gap_up_pct,
                    'ltp': ltp,
                    'timestamp': message['received_at']
                }
                
                if len(filtered_stocks) > MAX_FILTERED_STOCKS:
                    sorted_stocks = sorted(
                        filtered_stocks.items(),
                        key=lambda x: x[1]['gap_up_pct'],
                        reverse=True
                    )
                    filtered_stocks.clear()
                    for sym, data in sorted_stocks[:MAX_FILTERED_STOCKS]:
                        filtered_stocks[sym] = data
                
                print(f"\n🟢 CANDIDATE: {symbol} | Gap: {gap_up_pct:.2f}% | Open: {open_price}")
                
                if filtered_stocks:
                    print(f"\n🏆 TOP {MAX_FILTERED_STOCKS}:")
                    sorted_current = sorted(
                        filtered_stocks.items(),
                        key=lambda x: x[1]['gap_up_pct'],
                        reverse=True
                    )
                    for rank, (sym, data) in enumerate(sorted_current, 1):
                        print(f"   {rank}. {sym:20s} | Gap: {data['gap_up_pct']:.2f}%")
                print("-" * 80)
            
            gap_up_checked.add(symbol)
            
            if len(gap_up_checked) >= len(historical_close_prices):
                print("\n" + "=" * 80)
                print("✅ ALL STOCKS EVALUATED - SELECTION COMPLETE")
                print("=" * 80)
                
                if fyers_ws_instance:
                    try:
                        fyers_ws_instance.unsubscribe(symbols=NSE_STOCKS, data_type="SymbolUpdate")
                    except Exception:
                        pass
        
        print(f"\r[{message_count:6d}] {symbol:20s} | LTP: {ltp:>8} | Vol: {volume:>10}", 
              end='', flush=True)


def onerror(message):
    """Handle WebSocket errors."""
    print(f"\n❌ WebSocket Error: {message}")


def onclose(message):
    """Handle WebSocket close and prepare payload in LOCAL VARIABLE."""
    global websocket_start_time, message_count, filtered_stocks, selected_stocks_payload
    
    print(f"\n🔌 WebSocket Closed: {message}")
    
    if websocket_start_time:
        duration = (datetime.now() - websocket_start_time).total_seconds()
        print(f"\n📊 Session: {duration:.2f}s | Messages: {message_count:,}")
    
    print("\n" + "=" * 80)
    print(f"🎯 FINAL SELECTION - TOP {MAX_FILTERED_STOCKS} STOCKS")
    print("=" * 80)
    
    if not filtered_stocks:
        print("\n⚠️ No stocks selected.")
        selected_stocks_payload = []
        return
    
    sorted_final = sorted(
        filtered_stocks.items(),
        key=lambda x: x[1]['gap_up_pct'],
        reverse=True
    )
    
    # BUILD PAYLOAD AND STORE IN LOCAL VARIABLE (NOT DISK/RAM STORAGE)
    selected_stocks_payload = []
    for rank, (symbol, data) in enumerate(sorted_final[:MAX_FILTERED_STOCKS], 1):
        if EXECUTION_MODE == "ZERODHA":
            trading_symbol = convert_fyers_to_zerodha_symbol(symbol)
        else:
            trading_symbol = symbol
        
        payload = {
            "rank": rank,
            "fyers_symbol": symbol,
            "trading_symbol": trading_symbol,
            "exchange": "NSE",
            "qty": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "product": "MIS",
            "prev_close": data["prev_close"],
            "open_price": data["open_price"],
            "gap_up_pct": data["gap_up_pct"],
            "ltp": data["ltp"],
            "timestamp": data["timestamp"],
            "execution_mode": EXECUTION_MODE
        }
        
        selected_stocks_payload.append(payload)
        print(f"{rank}. {symbol} | Gap: {data['gap_up_pct']:.2f}% | Mode: {EXECUTION_MODE}")
    
    print("\n✅ Payload stored in local variable: selected_stocks_payload")
    print("=" * 80 + "\n")


def onopen(fyers_ws):
    """Handle WebSocket open and subscribe to stocks."""
    global websocket_start_time, message_count, fyers_ws_instance
    
    fyers_ws_instance = fyers_ws
    
    print("\n✅ WebSocket Connected!")
    print("=" * 80)
    
    websocket_start_time = datetime.now()
    message_count = 0
    
    print(f"\n📡 Subscribing to {len(NSE_STOCKS)} stocks...")
    print(f"📅 Start: {websocket_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "=" * 80)
    
    try:
        fyers_ws.subscribe(symbols=NSE_STOCKS, data_type="SymbolUpdate")
        print(f"\n✅ Subscribed to {len(NSE_STOCKS)} stocks!")
        print("\n📊 Streaming live data...\n")
        print("-" * 80)
        
        fyers_ws.keep_running()
        
    except Exception as e:
        print(f"\n❌ Subscription Error: {str(e)}")


def run_stock_selection(historical_data):
    """Start live data streaming and stock selection."""
    global live_data_buffer, message_count, historical_close_prices, filtered_stocks, gap_up_checked
    
    print("\n" + "=" * 80)
    print("STEP 2: STOCK SELECTION (Fyers WebSocket)")
    print("=" * 80)
    
    filtered_stocks.clear()
    gap_up_checked.clear()
    
    if historical_data is not None and not historical_data.empty:
        for _, row in historical_data.iterrows():
            historical_close_prices[row['Symbol']] = row['Close']
        
        print(f"\n✅ Loaded {len(historical_close_prices)} historical prices")
        print(f"🎯 Filters: Gap {GAP_UP_MIN}%-{GAP_UP_MAX}% | Min Price ₹{MIN_STOCK_PRICE}")
    
    print("\n" + "=" * 80)
    print("⏰ WAITING FOR 9:10 AM")
    print("=" * 80)
    wait_for_market_time(target_hour=9, target_minute=10)
    print("=" * 80)
    
    try:
        token = get_stored_token()
        access_token = f"{APP_ID}-{APP_TYPE}:{token}"
        
        print("\n🔌 Connecting to Fyers WebSocket...")
        
        fyers_ws = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=lambda: onopen(fyers_ws),
            on_close=onclose,
            on_error=onerror,
            on_message=onmessage
        )
        
        fyers_ws.connect()
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")


# ============================================================================
# STEP 3: ORDER EXECUTION AT 9:15 AM
# ============================================================================

def wait_for_execution_time():
    """Wait until 9:15 AM for order execution."""
    now = datetime.now()
    target_time = now.replace(
        hour=ORDER_EXECUTION_HOUR,
        minute=ORDER_EXECUTION_MINUTE,
        second=0,
        microsecond=0
    )
    
    # If already past execution time, return immediately
    if now >= target_time:
        print(f"✅ Current time {now.strftime('%H:%M:%S')} is past execution time")
        return
    
    wait_seconds = (target_time - now).total_seconds()
    
    print(f"\n⏰ Current time: {now.strftime('%H:%M:%S')}")
    print(f"⏰ Execution time: {ORDER_EXECUTION_HOUR:02d}:{ORDER_EXECUTION_MINUTE:02d}:00")
    print(f"⏰ Waiting for {wait_seconds/60:.1f} minutes...\n")
    
    start_wait = datetime.now()
    while datetime.now() < target_time:
        elapsed = (datetime.now() - start_wait).total_seconds()
        remaining = wait_seconds - elapsed
        
        if remaining > 0:
            print(f"\r⏳ Countdown to execution: {remaining:.0f} seconds remaining", end='', flush=True)
        
        time.sleep(1)
    
    print(f"\n\n🚀 EXECUTION TIME REACHED: {datetime.now().strftime('%H:%M:%S')}\n")


import time

def execute_fyers_orders(payload_list):
    global fyers_client
    
    print("\n" + "=" * 80)
    print("📤 EXECUTING ORDERS - FYERS API")
    print("=" * 80)
    
    for payload in payload_list:
        try:
            order_data = {
                "symbol": payload["symbol"],
                "qty": payload["qty"],
                "type": 2,  # MARKET
                "side": 1,  # BUY
                "productType": "INTRADAY"
            }

            # ⏱ START TIMER
            t_start = time.perf_counter()

            response = fyers_client.place_basket_orders(data=order_data)

            # ⏱ END TIMER
            t_end = time.perf_counter()

            latency_ms = (t_end - t_start) * 1000

            print(f"\n🔄 Order: {payload['symbol']}")
            print(f"⏱ API Latency: {latency_ms:.2f} ms")
            print("order payload passed: ", payload)

            if response.get("s") == "ok":
                print("✅ Order placed successfully")
                print(f"   Order ID: {response.get('id', 'N/A')}")
            else:
                print(f"❌ Order failed: {response.get('message', 'Unknown error')}")

        except Exception as e:
            print(f"❌ Exception for {payload['symbol']}: {str(e)}")
    
    print("\n" + "=" * 80)



def execute_zerodha_orders(payload_list):
    """Execute orders using Zerodha Kite API."""
    print("\n" + "=" * 80)
    print("📤 EXECUTING ORDERS - ZERODHA KITE API")
    print("=" * 80)
    
    try:
        # Import Zerodha Kite
        from kiteconnect import KiteConnect
        
        # Get Zerodha credentials (you need to set these)
        api_key = os.getenv("ZERODHA_API_KEY")
        access_token = os.getenv("ZERODHA_ACCESS_TOKEN")
        
        if not api_key or not access_token:
            raise Exception(
                "Zerodha credentials not found!\n"
                "Set ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN environment variables"
            )
        
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        for payload in payload_list:
            try:
                print(f"\n🔄 Placing order: {payload['trading_symbol']} (Rank {payload['rank']})")
                print(f"   Gap-up: {payload['gap_up_pct']:.2f}% | Qty: {payload['qty']}")
                
                order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=payload["trading_symbol"],
                    transaction_type=kite.TRANSACTION_TYPE_BUY,
                    quantity=payload["qty"],
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET
                )
                
                print(f"✅ Order placed successfully!")
                print(f"   Order ID: {order_id}")
                
            except Exception as e:
                print(f"❌ Exception placing order for {payload['trading_symbol']}: {str(e)}")
        
    except ImportError:
        print("\n❌ Zerodha KiteConnect not installed!")
        print("   Install: pip install kiteconnect")
    except Exception as e:
        print(f"\n❌ Zerodha execution error: {str(e)}")
    
    print("\n" + "=" * 80)


def execute_orders():
    """
    Step 3: Execute orders at 9:15 AM
    Uses payload from local variable (selected_stocks_payload)
    """
    global selected_stocks_payload
    
    print("\n" + "=" * 80)
    print("STEP 3: ORDER EXECUTION")
    print("=" * 80)
    
    # Check if we have stocks to trade
    if not selected_stocks_payload:
        print("\n⚠️ No stocks in payload. Skipping execution.")
        return
    
    print(f"\n📊 Stocks in payload: {len(selected_stocks_payload)}")
    print(f"⚙️  Execution mode: {EXECUTION_MODE}")
    
    for idx, stock in enumerate(selected_stocks_payload, 1):
        print(f"   {idx}. {stock['symbol']} | Qty: {stock['qty']}")
    production_mode = True
    if(production_mode):
       wait_for_execution_time()
    # Wait until 9:15 AM
    
    
    # Execute based on mode
    if EXECUTION_MODE == "ZERODHA":
        execute_zerodha_orders(selected_stocks_payload)
    else:  # FYERS
        execute_fyers_orders(selected_stocks_payload)
    
    print("\n✅ Order execution completed!")


# ============================================================================
# MAIN EXECUTION FLOW
# ============================================================================

def main():
    """
    Main execution flow:
    1. Authenticate with Fyers (always required for data)
    2. Run stock selection (Fyers WebSocket)
    3. Execute orders at 9:15 AM (Fyers/Zerodha based on mode)
    """
    
    print("\n" + "=" * 100)
    print(" " * 30 + "🚀 ALGORITHMIC TRADING SYSTEM")
    print("=" * 100)
    print(f"\n⚙️  Configuration:")
    print(f"   • Data Source: Fyers API (always)")
    print(f"   • Execution Mode: {EXECUTION_MODE}")
    print(f"   • Max Stocks: {MAX_FILTERED_STOCKS}")
    print(f"   • Gap-up Range: {GAP_UP_MIN}% - {GAP_UP_MAX}%")
    print(f"   • Min Stock Price: ₹{MIN_STOCK_PRICE}")
    print(f"   • Execution Time: {ORDER_EXECUTION_HOUR:02d}:{ORDER_EXECUTION_MINUTE:02d} AM")
    print("\n" + "=" * 100)
    
    try:
        # STEP 1: Authenticate with Fyers
        authenticate_fyers()
        Production_Mode = False
        if(Production_Mode):
            # STEP 2: Fetch historical data
            print("\n📥 Fetching last trading day data...")
            df = fetch_last_trading_day_all_stocks()
            #printing historical data if
            print("\n historical data : ", df)
            if df.empty:
                print("\n❌ No historical data fetched. Exiting.")
                return
        
        # STEP 3: Run stock selection (stores result in selected_stocks_payload)
        
        if(Production_Mode):
            run_stock_selection(historical_data=df)
        
        
        # STEP 4: Execute orders at 9:15 AM
        execute_orders()
        
        print("\n" + "=" * 100)
        print(" " * 35 + "✅ SYSTEM COMPLETED")
        print("=" * 100 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ System interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ System error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup_fyers_logs()


if __name__ == "__main__":
    main()