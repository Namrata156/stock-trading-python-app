import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()


POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000

url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
response = requests.get(url)

tickers = []
data = response.json()
print(data)
print(data['next_url'])

for ticker in data['results']:
    tickers.append(ticker)

max_pages = 4
pages_fetched = 0

while 'next_url' in data and pages_fetched< max_pages:
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data.keys())
    # print(data)

    if data['results']:
        for ticker in data['results']:
             tickers.append(ticker)

    pages_fetched += 1

print(len(tickers))
print(f"Page {pages_fetched+1}: collected {len(tickers)} tickers so far")

example_ticker = {
                'ticker': 'BAUG', 
                'name': 'Innovator U.S. Equity Buffer ETF - August', 
                'market': 'stocks', 
                'locale': 'us', 
                'primary_exchange': 'BATS', 
                'type': 'ETF', 
                'active': True, 
                'currency_name': 'usd', 
                'cik': '0001482688', 
                'composite_figi': 'BBG00PVP2Q68', 
                'share_class_figi': 'BBG00PVP2QY7', 
                'last_updated_utc': '2025-09-20T06:05:17.341691359Z'
                }

# Write tickers to csv

fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'
with open(output_csv, mode = 'w', newline= '', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key,'') for key in fieldnames}
        writer.writerow(row)
print(f'Wrote {len(tickers)} rows to {output_csv}')