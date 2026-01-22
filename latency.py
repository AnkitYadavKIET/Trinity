import requests
import time

url = "https://api.fyers.in/api/v2/profile"  # Example endpoint
headers = {
    "Authorization":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCcGNsQ0FkeE5oMXhRbzhldzdMUmxlOEd3RE1Cemx2UTVmM0N2ajkyempyVGR5SzlUUXFFUFVnYXJPVFg0VUNUWHJsTG9qNEstdEFBLWgyVXVGNW5KbkxNcWpFNFZyZHlYcGdVT1dYQjloQkFoeV9TWT0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJhOTUzNjMzZTQzYzIwNmJlMDM5ZmQ1N2MzNjNmNThjYTA5ZjAwZTEyMmRiNzUwZWM4ZTJkMWU5ZCIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVAxOTY5OCIsImFwcFR5cGUiOjEwMCwiZXhwIjoxNzY5MTI4MjAwLCJpYXQiOjE3NjkwOTkzOTIsImlzcyI6ImFwaS5meWVycy5pbiIsIm5iZiI6MTc2OTA5OTM5Miwic3ViIjoiYWNjZXNzX3Rva2VuIn0.n5KC_m5xvKxD7ydAJJgBceGpKfAxTd_Hk_uOjtpAV2k"
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
