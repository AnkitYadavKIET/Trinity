
from kiteconnect import KiteConnect
import webbrowser

API_KEY="YOUR_API_KEY"
API_SECRET="YOUR_API_SECRET"

kite=KiteConnect(api_key=API_KEY)
print("Opening login...")
webbrowser.open(kite.login_url())
req=input("Paste request_token: ").strip()
data=kite.generate_session(req, api_secret=API_SECRET)
print("ACCESS TOKEN:", data["access_token"])
