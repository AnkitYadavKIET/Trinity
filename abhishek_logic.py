"""
Fyers Order Placement Module
Uses the official Fyers API v3 to place basket orders.
Optimized for low-latency order execution.
"""

from fyers_apiv3 import fyersModel
import os
import time
import sys
import json
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Import connection utilities from fyers_connection
from fyers_connection import (
    APP_ID,
    APP_TYPE,
    get_stored_token,
    is_token_valid
)


def get_fyers_client_manual():
    print("\n🔑 ENTER FYERS ACCESS TOKEN (paste & press Enter)")
    token = input("Token: ").strip()

    if not token:
        print("❌ Empty token")
        return None

    client_id = f"{APP_ID}-{APP_TYPE}"

    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=token,
        is_async=False,
        log_path=""
    )

    print("✅ Fyers client initialized (manual token)")
    return fyers



def place_basket_orders(fyers, orders_data):
    """
    Place multiple orders using Fyers basket orders API.
    
    Args:
        fyers: FyersModel instance
        orders_data: List of order dictionaries with the following structure:
            - symbol: NSE symbol (e.g., "NSE:SBIN-EQ")
            - qty: Quantity
            - type: Order type (1=Limit, 2=Market, 3=Stop, 4=Stoplimit)
            - side: 1=Buy, -1=Sell
            - productType: "INTRADAY", "CNC", "MARGIN", "CO", "BO"
            - limitPrice: Limit price (0 for market orders)
            - stopPrice: Stop price (0 for market orders)
            - validity: "DAY" or "IOC"
            - disclosedQty: Disclosed quantity (0 for full disclosure)
            - offlineOrder: False for online orders
    
    Returns:
        API response dictionary
    """
    if not fyers:
        print("❌ Fyers client is not initialized.")
        return None
    
    if not orders_data or len(orders_data) == 0:
        print("❌ No orders to place.")
        return None
    
    print(f"\n📦 Placing basket order with {len(orders_data)} orders...")
    
    # Place basket orders using official API
    response = fyers.place_basket_orders(data=orders_data)
    
    return response


def parse_basket_response(response):
    """
    Parse and display the basket order response.
    
    Args:
        response: API response from place_basket_orders
    """
    if not response:
        print("❌ No response received.")
        return
    
    print("\n" + "=" * 60)
    print("📊 BASKET ORDER RESPONSE")
    print("=" * 60)
    
    # Check overall status
    if response.get('s') == 'ok':
        print(f"✅ Overall Status: SUCCESS (Code: {response.get('code', 'N/A')})")
    else:
        print(f"❌ Overall Status: {response.get('s', 'UNKNOWN')} (Code: {response.get('code', 'N/A')})")
        if response.get('message'):
            print(f"   Message: {response.get('message')}")
    
    # Parse individual order responses
    data = response.get('data', [])
    if data:
        print(f"\n📋 Individual Order Results ({len(data)} orders):")
        print("-" * 60)
        
        success_count = 0
        error_count = 0
        
        for i, order_result in enumerate(data, 1):
            status_code = order_result.get('statusCode', 'N/A')
            status_desc = order_result.get('statusDescription', 'N/A')
            body = order_result.get('body', {})
            
            order_status = body.get('s', 'unknown')
            order_message = body.get('message', 'No message')
            order_id = body.get('id', 'N/A')
            
            if status_code == 200 and order_status == 'ok':
                success_count += 1
                print(f"  ✅ Order {i}: SUCCESS")
                print(f"      Order ID: {order_id}")
                print(f"      Message: {order_message}")
            else:
                error_count += 1
                print(f"  ❌ Order {i}: FAILED")
                print(f"      Status: {status_code} - {status_desc}")
                print(f"      Error: {order_message}")
            print()
        
        print("-" * 60)
        print(f"📈 Summary: {success_count} successful, {error_count} failed")
    else:
        print("⚠️ No order data in response.")
    
    print("=" * 60)


def create_order(symbol, qty, side, order_type=2, product_type="INTRADAY", 
                 limit_price=0, stop_price=0, validity="DAY", 
                 disclosed_qty=0, offline_order=False):
    """
    Helper function to create an order dictionary.
    
    Args:
        symbol: NSE symbol (e.g., "NSE:SBIN-EQ")
        qty: Quantity to buy/sell
        side: 1 for Buy, -1 for Sell
        order_type: 1=Limit, 2=Market, 3=Stop, 4=Stoplimit (default: 2=Market)
        product_type: "INTRADAY", "CNC", "MARGIN", "CO", "BO" (default: "INTRADAY")
        limit_price: Limit price (default: 0 for market orders)
        stop_price: Stop price (default: 0 for market orders)
        validity: "DAY" or "IOC" (default: "DAY")
        disclosed_qty: Disclosed quantity (default: 0)
        offline_order: Offline order flag (default: False)
    
    Returns:
        Order dictionary
    """
    return {
        "symbol": symbol,
        "qty": qty,
        "type": order_type,
        "side": side,
        "productType": product_type,
        "limitPrice": limit_price,
        "stopPrice": stop_price,
        "validity": validity,
        "disclosedQty": disclosed_qty,
        "offlineOrder": offline_order
    }


def get_scheduled_time():
    """
    Ask user for scheduled order execution time.
    Returns datetime object for the scheduled time.
    """
    print("\n" + "=" * 60)
    print("⏰ SCHEDULED ORDER EXECUTION")
    print("=" * 60)
    
    current_time = datetime.now()
    print(f"📍 Current Time: {current_time.strftime('%H:%M:%S')}")
    print("\nEnter the time when you want to place the orders.")
    print("Format: HH:MM:SS (24-hour format)")
    print("Example: 09:15:00 for 9:15 AM, 14:30:00 for 2:30 PM")
    print("-" * 60)
    
    while True:
        time_input = input("🕐 Enter order execution time (HH:MM:SS): ").strip()
        
        try:
            # Parse the input time
            scheduled_time = datetime.strptime(time_input, "%H:%M:%S")
            
            # Combine with today's date
            scheduled_datetime = datetime.now().replace(
                hour=scheduled_time.hour,
                minute=scheduled_time.minute,
                second=scheduled_time.second,
                microsecond=0
            )
            
            # If the time has already passed today, inform user
            if scheduled_datetime <= datetime.now():
                print(f"⚠️  The time {time_input} has already passed today!")
                retry = input("Do you want to enter a different time? (y/n): ").strip().lower()
                if retry == 'y':
                    continue
                else:
                    print("❌ Exiting...")
                    return None
            
            print(f"\n✅ Orders will be placed at: {scheduled_datetime.strftime('%H:%M:%SS')}")
            return scheduled_datetime
            
        except ValueError:
            print("❌ Invalid time format! Please use HH:MM:SS format (e.g., 09:15:00)")


def warm_connection(fyers, silent=False):
    """
    Pre-warm the HTTP connection by making a lightweight API call.
    This establishes the TCP connection and TLS handshake in advance.
    
    Args:
        fyers: FyersModel instance
        silent: If True, don't print messages
    
    Returns:
        True if successful, False otherwise
    """
    try:
        if not silent:
            print("🔥 Warming up connection...")
        
        # Make a lightweight API call to establish connection
        start = time.perf_counter()
        _ = fyers.get_profile()
        elapsed = (time.perf_counter() - start) * 1000
        
        if not silent:
            print(f"✅ Connection warmed up in {elapsed:.2f}ms")
        return True
    except Exception as e:
        if not silent:
            print(f"⚠️ Connection warm-up failed: {e}")
        return False


def keep_connection_alive(fyers, stop_event, interval=5):
    """
    Background thread to keep the connection alive with periodic pings.
    
    Args:
        fyers: FyersModel instance
        stop_event: threading.Event to signal stop
        interval: Seconds between pings
    """
    while not stop_event.is_set():
        try:
            _ = fyers.get_profile()
        except:
            pass
        # Wait for interval or until stop event
        stop_event.wait(timeout=interval)


def display_countdown(target_time, fyers=None):
    """
    Display a countdown timer until the target time.
    Updates in place on the terminal.
    Warms connection when approaching target time.
    
    Args:
        target_time: datetime object for when to stop countdown
        fyers: Optional FyersModel instance for connection warming
    
    Returns:
        True when countdown completes
    """
    print("\n" + "=" * 60)
    print("⏳ COUNTDOWN TO ORDER EXECUTION")
    print("=" * 60)
    
    connection_warmed = False
    last_warm_time = 0
    
    while True:
        now = datetime.now()
        remaining = target_time - now
        remaining_ms = remaining.total_seconds() * 1000
        
        if remaining_ms <= 0:
            # Clear the countdown line and show completion
            sys.stdout.write("\r" + " " * 60 + "\r")
            sys.stdout.flush()
            print("🚀 TIME REACHED! Executing orders NOW!")
            print("=" * 60)
            return True
        
        # Warm connection 3 seconds before execution, then every 1 second
        if fyers and remaining_ms <= 3000:
            current_time = time.perf_counter()
            if current_time - last_warm_time >= 0.8:  # Warm every 800ms
                warm_connection(fyers, silent=True)
                last_warm_time = current_time
                if not connection_warmed:
                    sys.stdout.write("\r" + " " * 60 + "\r")
                    print("🔥 Connection pre-warmed for fast execution!")
                    connection_warmed = True
        
        # Calculate hours, minutes, seconds
        total_seconds = int(remaining.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        # Display countdown with carriage return to update in place
        countdown_str = f"⏱️  Time Remaining: {hours:02d}:{minutes:02d}:{seconds:02d}"
        sys.stdout.write(f"\r{countdown_str}")
        sys.stdout.flush()
        
        # Use tighter loop when close to target time for precision
        if remaining_ms <= 100:
            # Busy wait for last 100ms for precision
            pass
        elif remaining_ms <= 1000:
            time.sleep(0.01)  # 10ms sleep for last second
        else:
            time.sleep(0.1)  # 100ms sleep otherwise


def measure_order_speed(fyers, orders_data):
    """
    Place orders and measure the execution speed.
    
    Args:
        fyers: FyersModel instance
        orders_data: List of order dictionaries
    
    Returns:
        Tuple of (response, execution_time_ms)
    """
    print(f"\n📦 Placing basket order with {len(orders_data)} orders...")
    
    # Record start time with high precision
    start_time = time.perf_counter()
    
    # Place basket orders
    response = fyers.place_basket_orders(data=orders_data)
    
    # Record end time
    end_time = time.perf_counter()
    
    # Calculate execution time in milliseconds
    execution_time_ms = (end_time - start_time) * 1000
    
    return response, execution_time_ms


def display_order_speed(execution_time_ms, num_orders):
    """
    Display order execution speed statistics.
    
    Args:
        execution_time_ms: Total execution time in milliseconds
        num_orders: Number of orders placed
    """
    print("\n" + "=" * 60)
    print("⚡ ORDER EXECUTION SPEED")
    print("=" * 60)
    
    # Format time appropriately
    if execution_time_ms >= 1000:
        print(f"🕐 Total Execution Time: {execution_time_ms:.2f} ms ({execution_time_ms/1000:.3f} seconds)")
    else:
        print(f"🕐 Total Execution Time: {execution_time_ms:.2f} ms")
    
    print(f"📊 Number of Orders: {num_orders}")
    
    if num_orders > 0:
        avg_time_per_order = execution_time_ms / num_orders
        print(f"📈 Average Time per Order: {avg_time_per_order:.2f} ms")
    
    # Speed rating
    if execution_time_ms < 100:
        speed_rating = "🚀 ULTRA FAST"
    elif execution_time_ms < 300:
        speed_rating = "⚡ VERY FAST"
    elif execution_time_ms < 500:
        speed_rating = "✅ FAST"
    elif execution_time_ms < 1000:
        speed_rating = "👍 GOOD"
    else:
        speed_rating = "🐢 SLOW"
    
    print(f"🏆 Speed Rating: {speed_rating}")
    print("=" * 60)


def fire_order_only(fyers, orders_data):
    """
    Fire order with ZERO processing - just the API call.
    This is called at the exact scheduled time.
    
    Args:
        fyers: Pre-initialized FyersModel instance
        orders_data: Pre-prepared order data
    
    Returns:
        Tuple of (response, execution_time_ms)
    """
    # ONLY the API call - nothing else
    start_time = time.perf_counter()
    response = fyers.place_basket_orders(data=orders_data)
    end_time = time.perf_counter()
    
    execution_time_ms = (end_time - start_time) * 1000
    return response, execution_time_ms


def wait_and_fire_at_exact_time(target_time, fyers, orders_data, early_fire_ms=0):
    """
    Low-latency order fire at exact target time (or early by offset).
    Two-phase: wall-clock coarse wait with connection warming, then perf_counter spin.
    
    Args:
        target_time: datetime for the scheduled/display time
        fyers: FyersModel instance
        orders_data: Order data list
        early_fire_ms: Milliseconds to fire BEFORE target_time (to compensate for latency)
    
    Returns:
        Tuple of (response, execution_time_ms, fire_time, response_time, delay_ms, early_fire_ms)
    """
    # Pre-bind hot references
    _place_orders = fyers.place_basket_orders
    _data = orders_data
    _perf = time.perf_counter
    _sleep = time.sleep
    _time = time.time
    _get_profile = fyers.get_profile  # For connection warming
    
    # Target as unix timestamp - subtract early fire offset
    # If early_fire_ms=100, we fire 100ms BEFORE target_time
    adjusted_target_time = target_time - timedelta(milliseconds=early_fire_ms)
    target_ts = adjusted_target_time.timestamp()
    
    # Phase 1: Coarse sleep using wall clock (reliable)
    # Warm connection periodically to keep HTTP connection pool hot
    last_warm = 0
    while True:
        remaining = target_ts - _time()
        if remaining <= 0.2:
            break
        
        # Warm connection every 500ms, but stop 500ms before target
        current = _time()
        if remaining > 0.5 and current - last_warm >= 0.5:
            try:
                _get_profile()  # Keeps connection alive
            except:
                pass
            last_warm = current
        
        if remaining > 2.0:
            _sleep(0.5)
        elif remaining > 0.5:
            _sleep(0.1)
        else:
            _sleep(0.01)
    
    # Phase 2: Convert to perf_counter for final precision
    # Sync wall clock to perf_counter at this moment
    sync_ts = _time()
    sync_pc = _perf()
    target_pc = sync_pc + (target_ts - sync_ts)
    
    # Tight busy-spin until target
    while _perf() < target_pc:
        pass
    
    # Fire - zero overhead
    t0 = _perf()
    response = _place_orders(data=_data)
    t1 = _perf()
    
    # Post-fire measurements
    execution_time_ms = (t1 - t0) * 1000
    # delay_ms is relative to the ADJUSTED target (early fire time)
    delay_from_adjusted_ms = (t0 - target_pc) * 1000
    
    # Calculate actual fire time and when response arrived
    fire_time = adjusted_target_time + timedelta(milliseconds=delay_from_adjusted_ms)
    response_time = fire_time + timedelta(milliseconds=execution_time_ms)
    
    # delay_ms shows how far from the ORIGINAL target (for display)
    delay_from_original_ms = delay_from_adjusted_ms - early_fire_ms
    
    return response, execution_time_ms, fire_time, response_time, delay_from_original_ms, early_fire_ms


def main():
    """
    OPTIMIZED ORDER EXECUTION FLOW:
    1. Get scheduled time FIRST
    2. ALL preparation happens during wait time
    3. At execution moment, ONLY fire the API call
    4. Show pure API round-trip time
    """
    print("\n" + "=" * 60)
    print("🚀 FYERS LOW-LATENCY ORDER SYSTEM v2.0")
    print("=" * 60)
    print("📋 Flow: Time → Prepare → Wait → FIRE!")
    print("=" * 60)
    
    # ═══════════════════════════════════════════════════════════
    # STEP 1: GET SCHEDULED TIME FIRST
    # ═══════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("STEP 1: SET EXECUTION TIME")
    print("─" * 60)
    
    current_time = datetime.now()
    print(f"📍 Current Time: {current_time.strftime('%H:%M:%S')}")
    print("\nEnter the time when you want to place the orders.")
    print("Format: HH:MM:SS (24-hour format)")
    print("Example: 09:15:00 for 9:15 AM, 14:30:00 for 2:30 PM")
    
    while True:
        time_input = input("\n🕐 Enter order execution time (HH:MM:SS): ").strip()
        
        try:
            scheduled_time_parsed = datetime.strptime(time_input, "%H:%M:%S")
            scheduled_time = datetime.now().replace(
                hour=scheduled_time_parsed.hour,
                minute=scheduled_time_parsed.minute,
                second=scheduled_time_parsed.second,
                microsecond=0
            )
            
            if scheduled_time <= datetime.now():
                print(f"⚠️  Time {time_input} has already passed!")
                retry = input("Enter different time? (y/n): ").strip().lower()
                if retry != 'y':
                    print("❌ Exiting...")
                    return
                continue
            
            time_until = (scheduled_time - datetime.now()).total_seconds()
            print(f"\n✅ Orders will fire at: {scheduled_time.strftime('%H:%M:%S')}")
            print(f"⏱️  Time until execution: {int(time_until)} seconds")
            break
            
        except ValueError:
            print("❌ Invalid format! Use HH:MM:SS (e.g., 09:15:00)")
    
    # Ask for early fire offset
    print("\n" + "-" * 60)
    print("⚡ EARLY FIRE OFFSET (Latency Compensation)")
    print("-" * 60)
    print("To compensate for network latency, you can fire the order")
    print("a few milliseconds EARLY so it reaches the broker on time.")
    print("")
    print("Typical values:")
    print("  • 0 ms    = Fire exactly at target time (default)")
    print("  • 50 ms   = Fire 50ms early (fast connection)")
    print("  • 100 ms  = Fire 100ms early (normal connection)")
    print("  • 150 ms  = Fire 150ms early (slower connection)")
    print("-" * 60)
    
    while True:
        offset_input = input("🚀 Enter early fire offset in ms (0-500, default=0): ").strip()
        
        if offset_input == "":
            early_fire_ms = 0
            break
        
        try:
            early_fire_ms = int(offset_input)
            if early_fire_ms < 0 or early_fire_ms > 500:
                print("⚠️ Offset should be between 0 and 500 ms")
                continue
            break
        except ValueError:
            print("❌ Please enter a valid number (e.g., 100)")
    
    # Show adjusted fire time
    adjusted_fire_time = scheduled_time - timedelta(milliseconds=early_fire_ms)
    time_until = (adjusted_fire_time - datetime.now()).total_seconds()
    
    print(f"\n✅ Target time:      {scheduled_time.strftime('%H:%M:%S.000')}")
    if early_fire_ms > 0:
        print(f"🚀 Early fire offset: -{early_fire_ms} ms")
        print(f"⚡ Actual fire time:  {adjusted_fire_time.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"⏱️  Time until fire:  {time_until:.1f} seconds")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 2: PREPARE EVERYTHING (During wait time)
    # ═══════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("STEP 2: PREPARING (All processing happens now)")
    print("─" * 60)
    
    # 2a. Initialize Fyers client
    print("\n📡 Initializing Fyers client...")
    fyers = get_fyers_client_manual()
    if not fyers:
        print("❌ Failed to initialize Fyers client. Exiting.")
        return
    
    # 2b. Warm up connection
    print("\n🔥 Warming up connection...")
    warm_connection(fyers)
    
    # 2c. Prepare orders
    print("\n📝 Preparing order data...")
    orders_data = [
        create_order(
            symbol="NSE:SBIN-EQ",
            qty=100,
            side=-1,
            order_type=2,
            product_type="INTRADAY"
        ),
        create_order(
            symbol="NSE:TCS-EQ",
            qty=200,
            side=-1,
            order_type=2,
            product_type="INTRADAY"
        ),
    ]
    
    print("\n� Orders ready to fire:")
    for i, order in enumerate(orders_data, 1):
        side_str = "BUY" if order['side'] == 1 else "SELL"
        print(f"   {i}. {order['symbol']} - {side_str} {order['qty']} @ MARKET")
    
    # 2d. Start connection keeper
    print("\n🔄 Starting connection keep-alive...")
    stop_event = threading.Event()
    keeper_thread = threading.Thread(
        target=keep_connection_alive,
        args=(fyers, stop_event, 5),
        daemon=True
    )
    keeper_thread.start()
    
    # 2e. Final preparation status
    print("\n" + "─" * 60)
    print("✅ ALL PREPARATION COMPLETE!")
    print("─" * 60)
    print("   • Fyers client: Ready")
    print("   • Connection: Warmed")
    print("   • Orders: Pre-built")
    print("   • Keep-alive: Active")
    print("\n🎯 At execution time, ONLY the API call will be made.")
    print("   No processing, no delays - just FIRE!")
    
    # ═══════════════════════════════════════════════════════════
    # STEP 3: WAIT AND FIRE AT EXACT TIME
    # ═══════════════════════════════════════════════════════════
    print("\n" + "─" * 60)
    print("STEP 3: WAITING FOR SCHEDULED TIME")
    print("─" * 60)
    
    # This function waits AND fires the order at the exact time (or early)
    response, execution_time_ms, fire_time, response_time, delay_ms, used_offset = wait_and_fire_at_exact_time(
        scheduled_time, fyers, orders_data, early_fire_ms
    )
    
    # Stop keeper after firing
    stop_event.set()
    
    # ═══════════════════════════════════════════════════════════
    # RESULTS
    # ═══════════════════════════════════════════════════════════
    print("🚀 ORDER FIRED!")
    print(f"🎯 Target Time:    {scheduled_time.strftime('%H:%M:%S.000')}")
    if used_offset > 0:
        print(f"🚀 Early Offset:   -{used_offset} ms")
    print(f"⏰ Actual Fire:    {fire_time.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"📨 Response:       {response_time.strftime('%H:%M:%S.%f')[:-3]}")
    
    print("\n" + "=" * 60)
    print("⚡ TIMING RESULTS")
    print("=" * 60)
    
    # Show delay from target
    if delay_ms >= 0:
        delay_str = f"+{delay_ms:.2f}ms (late)"
    else:
        delay_str = f"{delay_ms:.2f}ms (early)"
    
    print(f"")
    print(f"   🎯 Fire Delay from Target: {delay_str}")
    print(f"   ⏱️  API Round-Trip Time:    {execution_time_ms:.2f} ms")
    print(f"   📊 Total (Delay + API):     {delay_ms + execution_time_ms:.2f} ms")
    print(f"")
    
    # Rating based on fire delay
    if abs(delay_ms) < 5:
        fire_rating = "🚀 PERFECT (<5ms)"
    elif abs(delay_ms) < 20:
        fire_rating = "⚡ EXCELLENT (<20ms)"
    elif abs(delay_ms) < 50:
        fire_rating = "✅ VERY GOOD (<50ms)"
    elif abs(delay_ms) < 100:
        fire_rating = "👍 GOOD (<100ms)"
    else:
        fire_rating = "⚠️ NEEDS IMPROVEMENT"
    
    print(f"   🏆 Fire Precision: {fire_rating}")
    print("=" * 60)
    
    # Parse response
    parse_basket_response(response)
    
    # Raw response
    print("\n🔍 Raw Response:")
    print(response)


if __name__ == "__main__":
    main()

