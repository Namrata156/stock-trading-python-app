def run_stock_job():
    import requests
    import os
    import csv
    from dotenv import load_dotenv
    import mysql.connector
    from datetime import datetime 
    load_dotenv()


    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

    LIMIT = 1000

    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)

    tickers = []
    data = response.json()
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

    # Add date_stamp to fieldnames
    fieldnames = list(example_ticker.keys()) + ['date_stamp']
    output_csv = 'tickers.csv'
    current_date = datetime.now().strftime('%Y-%m-%d')  # <-- Get current date

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in fieldnames if key != 'date_stamp'}
            row['date_stamp'] = current_date
            writer.writerow(row)
    print(f'Wrote {len(tickers)} rows to {output_csv}')

    # Connect to MySQL and verify connection
    conn = mysql.connector.connect(
        host="localhost",  # Use localhost when running on your host machine
        user="stock_user",
        password="namrata",
        database="stock_trading_db"
    )

    cursor = conn.cursor()

    # 1. Create table if not exists, add date_stamp column
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tickers (
        ticker VARCHAR(16) PRIMARY KEY,
        name VARCHAR(255),
        market VARCHAR(16),
        locale VARCHAR(16),
        primary_exchange VARCHAR(32),
        type VARCHAR(32),
        active BOOLEAN,
        currency_name VARCHAR(16),
        cik VARCHAR(32),
        composite_figi VARCHAR(32),
        share_class_figi VARCHAR(32),
        last_updated_utc VARCHAR(32),
        date_stamp DATE
    )
    """)

    # 2. Insert data with date_stamp
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames if key != 'date_stamp'}
        row['date_stamp'] = current_date
        cursor.execute("""
            INSERT INTO tickers (ticker, name, market, locale, primary_exchange, type, active, currency_name, cik, composite_figi, share_class_figi, last_updated_utc, date_stamp)
            VALUES (%(ticker)s, %(name)s, %(market)s, %(locale)s, %(primary_exchange)s, %(type)s, %(active)s, %(currency_name)s, %(cik)s, %(composite_figi)s, %(share_class_figi)s, %(last_updated_utc)s, %(date_stamp)s)
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
        """, row)

    conn.commit()
    print(f"Inserted/updated {len(tickers)} rows in MySQL.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_stock_job()
