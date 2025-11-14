import requests
import os
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

# Rate-limit / delay control (can be overridden via .env)
RATE_LIMIT_CALLS_PER_MIN = int(os.getenv("RATE_LIMIT_CALLS_PER_MIN", "5"))  # polygon free = 5
EXTRA_DELAY_SECONDS = float(os.getenv("EXTRA_DELAY_SECONDS", "0"))         # extra safety delay
MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))

MIN_INTERVAL = 60.0 / max(1, RATE_LIMIT_CALLS_PER_MIN)  # seconds between calls
MIN_INTERVAL += EXTRA_DELAY_SECONDS

_last_api_call_ts = 0.0  # module-level tracker


def make_api_call(raw_url, api_key, timeout=15):
    """
    Call Polygon API with rate limiting and retry/backoff.
    - ensures at least MIN_INTERVAL between calls
    - retries on 429 / transient errors with exponential backoff
    - appends apiKey if missing
    """
    global _last_api_call_ts

    # ensure apiKey is present
    if "apiKey=" not in raw_url:
        if "?" in raw_url:
            url = raw_url + f"&apiKey={api_key}"
        else:
            url = raw_url + f"?apiKey={api_key}"
    else:
        url = raw_url

    attempt = 0
    while attempt < MAX_RETRIES:
        # enforce minimum interval between calls
        now = time.time()
        elapsed = now - _last_api_call_ts
        if elapsed < MIN_INTERVAL:
            to_wait = MIN_INTERVAL - elapsed
            time.sleep(to_wait)

        try:
            resp = requests.get(url, timeout=timeout)
            # update timestamp immediately after request to account for call
            _last_api_call_ts = time.time()

            if resp.status_code == 429:
                # rate limited — wait and retry with backoff
                backoff = (2 ** attempt) * MIN_INTERVAL
                time.sleep(backoff)
                attempt += 1
                continue

            resp.raise_for_status()
            # optional: inspect headers like X-RateLimit-Remaining
            return resp.json()
        except requests.exceptions.RequestException as e:
            # transient network errors -> backoff and retry
            attempt += 1
            if attempt >= MAX_RETRIES:
                raise
            backoff = (2 ** (attempt - 1)) * MIN_INTERVAL
            time.sleep(backoff)

    # if we exit loop without returning, raise a generic error
    raise RuntimeError("API call failed after retries")


def run_stock_job():
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    if not POLYGON_API_KEY:
        print("POLYGON_API_KEY not found in environment or .env file.")
        return

    LIMIT = 1000
    base_url = f"https://api.polygon.io/v3/reference/tickers?market=otc&active=true&order=asc&limit={LIMIT}&sort=ticker"

    try:
        data = make_api_call(base_url, POLYGON_API_KEY)
    except Exception as e:
        print("Error fetching data from Polygon API:", e)
        return

    tickers = []
    tickers.extend(data.get("results", []))

    max_pages = 4
    pages_fetched = 0
    # follow next_url but respect rate limits via make_api_call
    while data.get("next_url") and pages_fetched < max_pages:
        next_url = data["next_url"]
        print("requesting next page", next_url)
        try:
            data = make_api_call(next_url, POLYGON_API_KEY)
        except Exception as e:
            print("Error fetching next page:", e)
            break

        tickers.extend(data.get("results", []))
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
    current_date = datetime.now().strftime('%Y-%m-%d')

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
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "stock_user"),
        password=os.getenv("DB_PASSWORD", "namrata"),
        database=os.getenv("DB_NAME", "stock_trading_db"),
    )

    cursor = conn.cursor()

    # recreate table (adjust as needed)
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
