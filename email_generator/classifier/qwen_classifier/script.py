import logging
import signal
import asyncio
import sys
from email_generator.database.supabase_client import db
from email_generator.classifier.qwen_classifier.qwen_labeler import (
    retry_failed_classifications,
    get_classification_stats,
    close_session
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('domain_classification.log'),
        logging.StreamHandler()
    ]
)

MAX_DOMAINS = 10000
BATCH_SIZE = 10
MAX_CONCURRENT = 3

stop_event = asyncio.Event()

async def handle_shutdown():
    logging.warning("Shutdown signal received. Stopping gracefully...")
    stop_event.set()

def signal_handler(signum, frame):
    asyncio.create_task(handle_shutdown())

async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logging.info("Retrying classification only for previously failed domains...")

        # Run the existing retry pipeline
        stats_before = get_classification_stats()
        logging.info(f"Initial classification stats: {stats_before}")

        results = await retry_failed_classifications(
            limit=MAX_DOMAINS,
            batch_size=BATCH_SIZE,
            max_concurrent=MAX_CONCURRENT
        )

        if not results:
            logging.info("No failed domains to retry.")
            return

        # Count successes vs errors
        success_count = sum(1 for r in results if r.category != "error")
        error_count = len(results) - success_count
        logging.info(f"Retry completed: {success_count} successful, {error_count} errors")

        # Category breakdown
        category_counts = {}
        for r in results:
            if r.category != "error":
                category_counts[r.category] = category_counts.get(r.category, 0) + 1
        if category_counts:
            logging.info("Category breakdown:")
            for category, count in sorted(category_counts.items()):
                logging.info(f"  {category}: {count}")

        stats_after = get_classification_stats()
        logging.info(f"Final classification stats: {stats_after}")
        logging.info("Failed domain retry pipeline completed successfully")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        await close_session()
        logging.info("Cleanup completed")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
