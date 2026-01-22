import json
import requests
import pyotp
import hashlib
import sys
from fyers_apiv3 import fyersModel
import time
import os

# === User-Specific Info ===
FY_ID = "YP19698"
APP_ID = "15F6FDRYP9"
APP_TYPE = "100"
PIN = "8484"
TOTP_KEY = "KLWB5CEQQLTQRG22B3WVJFBXGANA6L7D"
REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"  # Update this to your registered URI
SECRET_KEY = "X9HINMWFWM"

# === API Endpoints ===
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"

SUCCESS = 1
ERROR = -1

def send_login_otp(fy_id, app_id_type):
    payload = {"fy_id": fy_id, "app_id": app_id_type}
    res = requests.post(url=URL_SEND_LOGIN_OTP, json=payload)
    if res.status_code != 200:
        return [ERROR, res.text]
    return [SUCCESS, res.json()["request_key"]]

def generate_totp(secret):
    try:
        return [SUCCESS, pyotp.TOTP(secret).now()]
    except Exception as e:
        return [ERROR, e]

def verify_totp(request_key, totp):
    payload = {"request_key": request_key, "otp": totp}
    res = requests.post(url=URL_VERIFY_TOTP, json=payload)
    if res.status_code != 200:
        return [ERROR, res.text]
    return [SUCCESS, res.json()["request_key"]]

def verify_pin(request_key, pin):
    payload = {
        "request_key": request_key,
        "identity_type": "pin",
        "identifier": pin
    }
    res = requests.post(url=URL_VERIFY_PIN, json=payload)
    if res.status_code != 200:
        return [ERROR, res.text]
    return [SUCCESS, res.json()["data"]["access_token"]]

def generate_final_access_token(app_id, secret_key, pin_token, redirect_uri):
    client_id = app_id + "-" + APP_TYPE
    print(f"Using client_id: {client_id}")
    print(f"Using pin_token: {pin_token}")
    
    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type="code",
        grant_type="authorization_code",
        state="sample_state"
    )
    
    # Generate auth code URL
    auth_url = session.generate_authcode()
    print(f"\nPlease visit this URL to authorize the application:")
    print(auth_url)
    
    # Wait for user to visit URL and get auth code
    print("\nAfter authorizing, you will be redirected to a URL like:")
    print("https://127.0.0.1/?auth_code=YOUR_AUTH_CODE&state=sample_state")
    print("\nPlease copy ONLY the auth_code value (the part after auth_code= and before &state)")
    auth_code = input("\nEnter the auth code: ").strip()
    
    # Clean up the auth code - remove any extra parameters or whitespace
    if '&' in auth_code:
        auth_code = auth_code.split('&')[0]
    auth_code = auth_code.strip()
    
    print(f"Using auth code: {auth_code}")
    
    # Set the auth code and generate final access token
    session.set_token(auth_code)
    response = session.generate_token()
    print("Token Response:", response)
    
    # Check if response is successful and contains token
    if isinstance(response, dict):
        if response.get('s') == 'error':
            print(f"Error generating token: {response.get('message', 'Unknown error')}")
            raise Exception(f"Token generation failed: {response.get('message', 'Unknown error')}")
            
        if "access_token" in response:
            return response["access_token"]
        elif "token" in response:
            return response["token"]
        else:
            print("Available keys in response:", response.keys())
            raise Exception("No token found in response")
    else:
        print("Response type:", type(response))
        raise Exception("Invalid response format")

def get_stored_token():
    try:
        if os.path.exists("fyers_token.txt"):
            with open("fyers_token.txt", "r") as f:
                token = f.read().strip()
                if token:
                    return token
    except Exception as e:
        print(f"Error reading token: {e}")
    return None

def is_token_valid(token):
    try:
        if not token:
            return False
            
        client_id = f"{APP_ID}-{APP_TYPE}"
        fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=token,
            is_async=False,
            log_path=""
        )
        
        # Try to get profile to check if token is valid
        profile = fyers.get_profile()
        
        # Check if profile response is valid
        if not isinstance(profile, dict):
            print(f"Invalid profile response type: {type(profile)}")
            return False
            
        # Check if response has success status
        if 's' not in profile:
            print("Profile response missing 's' key")
            return False
            
        # Check if response is successful
        if profile['s'] != 'ok':
            print(f"Profile response not ok: {profile}")
            return False
            
        # If we get here, token is valid
        return True
        
    except Exception as e:
        print(f"Error validating token: {str(e)}")
        return False

def main():
    # First check if we have a stored token
    stored_token = get_stored_token()
    if stored_token and is_token_valid(stored_token):
        print("✅ Using existing token from fyers_token.txt")
        client_id = f"{APP_ID}-{APP_TYPE}"
        fyers = fyersModel.FyersModel(
            client_id=client_id,
            token=stored_token,
            is_async=False,
            log_path=""
        )
        profile = fyers.get_profile()
        print("🔍 Profile:", profile)
        return

    print("Getting new token...")
    
    # Step 1: OTP
    res_otp = send_login_otp(FY_ID, "2")
    if res_otp[0] != SUCCESS:
        print(f"Failed to send OTP: {res_otp[1]}")
        sys.exit()
    print("✅ OTP sent")

    # Step 2: TOTP
    res_totp = generate_totp(TOTP_KEY)
    if res_totp[0] != SUCCESS:
        print(f"Failed to generate TOTP: {res_totp[1]}")
        sys.exit()
    print("✅ TOTP generated")
    print(f"Debug - TOTP value: {res_totp[1]}")
    print(f"Debug - Request key from OTP: {res_otp[1]}")

    # Step 3: Verify TOTP
    res_verify_otp = verify_totp(res_otp[1], res_totp[1])
    if res_verify_otp[0] != SUCCESS:
        print(f"Failed to verify TOTP: {res_verify_otp[1]}")
        print(f"Debug - Full TOTP verification response: {res_verify_otp}")
        sys.exit()
    print("✅ TOTP verified")
    print(f"Debug - New request key after TOTP: {res_verify_otp[1]}")

    # Step 4: PIN
    res_pin = verify_pin(res_verify_otp[1], PIN)
    if res_pin[0] != SUCCESS:
        print(f"Failed to verify PIN: {res_pin[1]}")
        sys.exit()
    print("✅ PIN verified")

    # Step 5: Get final API token
    access_token = generate_final_access_token(APP_ID, SECRET_KEY, res_pin[1], REDIRECT_URI)
    print(f"🎉 Final Access Token: {access_token}")

    # Save token
    with open("fyers_token.txt", "w") as f:
        f.write(access_token)
        print("✅ Token saved to fyers_token.txt")

    time.sleep(2)

    # Step 6: Connect to Fyers API
    client_id = f"{APP_ID}-{APP_TYPE}"
    
    fyers = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,  # Use the raw access token
        is_async=False,
        log_path="",
    )

    # Set the access token
    fyers.token = access_token
    fyers.client_id = client_id
    
    profile = fyers.get_profile()
    print("🔍 Profile:", profile)

if __name__ == "__main__":
    main()
