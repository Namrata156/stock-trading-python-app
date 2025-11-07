import schedule
import time
import logging
from datetime import datetime
from script import run_stock_job

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_scheduler.log'),
        logging.StreamHandler()
    ]
)

def wrapped_stock_job():
    """Wrapper for metadata collection with error handling"""
    try:
        logging.info("Starting ticker metadata collection")
        run_stock_job()
        logging.info("Completed ticker metadata collection")
    except Exception as e:
        logging.error(f"Error collecting metadata: {str(e)}")

def main():
    # Run immediately first
    logging.info("Running initial job...")
    wrapped_stock_job()
    
    # Then schedule for midnight
    schedule.every().day.at("00:00").do(wrapped_stock_job)
    logging.info("Scheduled next run for midnight")
    
    while True:
        schedule.run_pending()
        time.sleep(300)  # Check every 5 minutes

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}")
