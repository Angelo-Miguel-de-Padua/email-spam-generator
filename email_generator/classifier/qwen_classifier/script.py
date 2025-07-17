import logging
import signal
import asyncio
import sys
from email_generator.database.supabase_client import db
from email_generator.classifier.qwen_classifier.qwen_labeler import (
    classify_unclassified_domains,
    get_classification_stats,
    retry_failed_classifications, 
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
MAX_CONCURRENT = 3
BATCH_SIZE = 10

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
        logging.info("Starting retry pipeline for failed domain classifications...")

        stats = get_classification_stats()
        logging.info(f"Initial classification stats: {stats}")

        logging.info(f"Retrying failed classifications for up to {MAX_DOMAINS} domains...")
        results = await retry_failed_classifications(
            limit=MAX_DOMAINS,
            batch_size=BATCH_SIZE,
            max_concurrent=MAX_CONCURRENT
        )

        success_count = sum(1 for r in results if r.category != "error")
        error_count = len(results) - success_count

        logging.info(f"Retry completed: {success_count} successful, {error_count} errors")

        if results:
            category_counts = {}
            for result in results:
                if result.category != "error":
                    category_counts[result.category] = category_counts.get(result.category, 0) + 1

            logging.info("Category breakdown:")
            for category, count in sorted(category_counts.items()):
                logging.info(f"  {category}: {count}")

        final_stats = get_classification_stats()
        logging.info(f"Final classification stats: {final_stats}")

        logging.info("Retry pipeline completed successfully")

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
