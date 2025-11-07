import requests
import os
import csv

from dotenv import load_dotenv
import mysql.connector
from datetime import datetime 

load_dotenv()

def run_stock_job():
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    if not POLYGON_API_KEY:
        print("POLYGON_API_KEY not found in environment or .env file.")
        return
    
    LIMIT = 1000
    url = f"https://api.polygon.io/v3/reference/tickers?market=otc&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data=response.json()
    except Exception as e:
        print("Error fetching data from Polygon API:", e)
        return

    tickers = []
    # use data already parsed above
    #print(data)
    #print(data['next_url'])

    for ticker in data['results']:
        tickers.append(ticker)

    max_pages = 4
    pages_fetched = 0

    while 'next_url' in data and pages_fetched < max_pages:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data.keys())
        #print(data)

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

    # Add date_stamp to fieldnames
    fieldnames = list(example_ticker.keys()) + ['date_stamp']
    output_csv = 'tickers_otc.csv'
    current_date = datetime.now().strftime('%Y-%m-%d')  # <-- Get current date

    # Append to CSV instead of overwriting; write header only when file is new
    write_header = not os.path.exists(output_csv)
    with open(output_csv, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in fieldnames if key != 'date_stamp'}
            row['date_stamp'] = current_date
            writer.writerow(row)
    print(f'Appended {len(tickers)} rows to {output_csv}')

    # Connect to MySQL and verify connection
    conn = mysql.connector.connect(
        host="localhost",  # Use localhost when running on your host machine
        user="stock_user",
        password="namrata",
        database="stock_trading_db"
    )

    cursor = conn.cursor()

    # First, drop and recreate table with consistent naming
    cursor.execute("DROP TABLE IF EXISTS tickers_otc")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tickers_otc (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(255) UNIQUE,
        name VARCHAR(500),
        market VARCHAR(100),
        locale VARCHAR(100),
        primary_exchange VARCHAR(32),
        type VARCHAR(32),
        active BOOLEAN,
        currency_name VARCHAR(100),
        cik VARCHAR(32),
        composite_figi VARCHAR(32),
        share_class_figi VARCHAR(32),
        last_updated_utc VARCHAR(32),
        date_stamp DATE,
        INDEX idx_ticker (ticker)
    )
    """)

    insert_sql = """
        INSERT INTO tickers_otc (
            ticker, name, market, locale, primary_exchange, 
            type, active, currency_name, cik, composite_figi, 
            share_class_figi, last_updated_utc, date_stamp
        ) VALUES (
            %(ticker)s, %(name)s, %(market)s, %(locale)s, 
            %(primary_exchange)s, %(type)s, %(active)s, 
            %(currency_name)s, %(cik)s, %(composite_figi)s, 
            %(share_class_figi)s, %(last_updated_utc)s, %(date_stamp)s
        )
        ON DUPLICATE KEY UPDATE
            name=VALUES(name),
            market=VALUES(market),
            locale=VALUES(locale),
            primary_exchange=VALUES(primary_exchange),
            type=VALUES(type),
            active=VALUES(active),
            currency_name=VALUES(currency_name),
            cik=VALUES(cik),
            composite_figi=VALUES(composite_figi),
            share_class_figi=VALUES(share_class_figi),
            last_updated_utc=VALUES(last_updated_utc),
            date_stamp=VALUES(date_stamp)
    """

    for t in tickers:
        row = {
            'ticker': t.get('ticker', ''),
            'name': t.get('name', ''),  
            'market': t.get('market', ''),
            'locale': t.get('locale', ''),
            'primary_exchange': t.get('primary_exchange', ''),
            'type': t.get('type', ''),
            'active': 1 if t.get('active') else 0,
            'currency_name': t.get('currency_name', ''),
            'cik': t.get('cik', ''),
            'composite_figi': t.get('composite_figi', ''),
            'share_class_figi': t.get('share_class_figi', ''),
            'last_updated_utc': t.get('last_updated_utc', ''),
            'date_stamp': current_date
        }
        cursor.execute(insert_sql, row)

    conn.commit()
    print(f"Inserted/updated {len(tickers)} rows in MySQL.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_stock_job()
