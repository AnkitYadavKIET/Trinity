
import socket, json
from datetime import datetime

HOST="127.0.0.1"
PORT=9009

def send(order):
    with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:
        s.connect((HOST,PORT))
        s.sendall(json.dumps(order).encode())
    print("Sent to Go:", order)

# Expecting two symbols from selection code
if __name__=="__main__":
    # Dummy example (selection script will import and call send())
    send({
        "symbol":"TCS",
        "exchange":"NSE",
        "qty":1,
        "side":"BUY",
        "order_type":"MARKET",
        "product":"MIS",
        "sent_at":datetime.now().isoformat()
    })
