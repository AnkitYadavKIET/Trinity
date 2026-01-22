import requests
import time

url = "https://api.fyers.in/api/v2/profile"  # Example endpoint
headers = {
    "Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBfaWQiOiIxNUY2RkRSWVA5IiwidXVpZCI6IjVhNmUxNjUwYTA0ODRjN2ZiODIwMDNjNGI4YTVjNDA1IiwiaXBBZGRyIjoiIiwibm9uY2UiOiIiLCJzY29wZSI6IiIsImRpc3BsYXlfbmFtZSI6IllQMTk2OTgiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJhOTUzNjMzZTQzYzIwNmJlMDM5ZmQ1N2MzNjNmNThjYTA5ZjAwZTEyMmRiNzUwZWM4ZTJkMWU5ZCIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImF1ZCI6IltcImQ6MVwiLFwiZDoyXCIsXCJ4OjBcIixcIng6MVwiLFwieDoyXCJdIiwiZXhwIjoxNzY5MTI3MzY4LCJpYXQiOjE3NjkwOTczNjgsImlzcyI6ImFwaS5sb2dpbi5meWVycy5pbiIsIm5iZiI6MTc2OTA5NzM2OCwic3ViIjoiYXV0aF9jb2RlIn0.TNJxe9oUCfG0HJiFFZfH3LMVvW9K0Md_OZeKoZZXneg"  # Optional if endpoint requires auth
}

latencies = []

for i in range(5):
    start = time.time()
    response = requests.get(url, headers=headers)  # Remove headers if public endpoint
    end = time.time()
    latencies.append((end - start) * 1000)
    print(f"Request {i+1}: {round((end - start)*1000, 2)} ms, Status: {response.status_code}")

avg_latency = sum(latencies)/len(latencies)
print(f"\nAverage latency: {round(avg_latency,2)} ms")
