import os
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from scrap_service.carlistmy_service_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv
from pathlib import Path

LOCK_FILE = Path("/tmp/carlistmy_scraping.lock")

def is_scraper_running():
    return LOCK_FILE.exists()

def set_scraper_lock():
    LOCK_FILE.touch()

def clear_scraper_lock():
    try:
        LOCK_FILE.unlink()
    except FileNotFoundError:
        pass

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

load_dotenv()

# Fungsi scraping untuk setiap cluster
def scrape_cluster(cluster_path: str, cluster_name: str):
    if is_scraper_running():
        logging.warning(f"⛔ {cluster_name} dijadwalkan tapi dibatalkan karena scraping lain masih berjalan.")
        return

    set_scraper_lock()

    os.environ["INPUT_FILE"] = cluster_path
    logging.info(f"▶️ Mulai scraping untuk {cluster_name}")

    scraper = CarlistMyService()
    try:
        scraper.scrape_all_brands()
    except Exception as e:
        logging.error(f"❌ Gagal scraping {cluster_name}: {e}")
    finally:
        scraper.close()
        clear_scraper_lock()
        logging.info(f"✅ Selesai scraping {cluster_name}")

scheduler = BlockingScheduler()

# Jadwal scraping berdasarkan jam
scheduler.add_job(
    scrape_cluster,
    'cron',
    hour=0,
    args=["scrap_service/carlistmy_service_playwright/storage/input_files/cluster_A.csv", "Cluster A"]
)

scheduler.add_job(
    scrape_cluster,
    'cron',
    hour=20,
    minute=14,
    args=["scrap_service/carlistmy_service_playwright/storage/input_files/cluster_B.csv", "Cluster B"]
)

scheduler.add_job(
    scrape_cluster,
    'cron',
    hour=12,
    args=["scrap_service/carlistmy_service_playwright/storage/input_files/cluster_C.csv", "Cluster C"]
)

scheduler.add_job(
    scrape_cluster,
    'cron',
    hour=18,
    args=["scrap_service/carlistmy_service_playwright/storage/input_files/cluster_D.csv", "Cluster D"]
)

# Start scheduler
if __name__ == "__main__":
    logging.info("⏱️ Scheduler aktif dan siap menjadwalkan scraping.")
    scheduler.start()
