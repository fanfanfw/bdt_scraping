import os
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path

from scrap_service.listing_tracker_service_carlistmy_playwright.database import get_database_connection

load_dotenv()

START_DATE = datetime.now().strftime('%Y%m%d')

base_dir = Path(__file__).resolve().parents[2]
log_dir = base_dir / "scraping" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"tracker_carlistmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("carlistmy_tracker")


def take_screenshot(page, name: str):
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_carlistmy_tracker"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"
        page.screenshot(path=str(screenshot_path))
        logger.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logger.warning(f"❌ Gagal menyimpan screenshot: {e}")

DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")

def get_custom_proxy_list():
    raw = os.getenv("CUSTOM_PROXIES", "")
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    parsed = []
    for p in proxies:
        try:
            ip, port, user, pw = p.split(":")
            parsed.append({
                "server": f"{ip}:{port}",
                "username": user,
                "password": pw
            })
        except ValueError:
            continue
    return parsed

def should_use_proxy():
    return (
        os.getenv("USE_PROXY", "false").lower() == "true"
        and os.getenv("PROXY_SERVER")
        and os.getenv("PROXY_USERNAME")
        and os.getenv("PROXY_PASSWORD")
    )

class ListingTrackerCarlistmyPlaywright:
    def __init__(self, batch_size=25):
        self.batch_size = batch_size
        self.sold_selector = "h2"
        self.sold_text_indicator = "This car has already been sold."
        self.active_selector = "h1"
        # Tambahkan proxies custom
        self.custom_proxies = get_custom_proxy_list()

    def init_browser(self):
        self.playwright = sync_playwright().start()

        launch_kwargs = {
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        }

        proxy_mode = os.getenv("PROXY_MODE", "none").lower()
        if proxy_mode == "oxylabs":
            launch_kwargs["proxy"] = {
                "server": os.getenv("PROXY_SERVER"),
                "username": os.getenv("PROXY_USERNAME"),
                "password": os.getenv("PROXY_PASSWORD")
            }
            logger.info("🌐 Proxy aktif (Oxylabs digunakan)")
        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            launch_kwargs["proxy"] = proxy
            logger.info(f"🌐 Proxy custom digunakan (random): {proxy['server']}")
        else:
            logger.info("⚡ Menjalankan browser tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur"
        )

        self.context.route(
            "**/*",
            lambda route, request: route.abort() if request.resource_type == "image" else route.continue_()
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logger.info("✅ Browser Playwright diinisialisasi.")

    def get_current_ip(self):
        try:
            self.page.goto('https://ip.oxylabs.io/', timeout=15000)
            ip_text = self.page.inner_text('body').strip()
            logger.info(f"🌐 IP yang digunakan: {ip_text}")
        except Exception as e:
            logger.error(f"Gagal mendapatkan IP saat ini: {e}")
            # Screenshot saat gagal
            take_screenshot(self.page, "failed_get_ip")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception:
            pass
        try:
            self.playwright.stop()
        except Exception:
            pass
        logger.info("🛑 Browser Playwright ditutup.")

    def random_delay(self, min_d=2, max_d=5):
        time.sleep(random.uniform(min_d, max_d))

    def update_car_status(self, car_id, status, sold_at=None):
        conn = get_database_connection()
        if not conn:
            logger.error("Tidak bisa update status, koneksi database gagal.")
            return
        cursor = conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {DB_TABLE_PRIMARY}
                SET status = %s, sold_at = %s, last_scraped_at = %s
                WHERE id = %s
            """, (status, sold_at, datetime.now(), car_id))
            conn.commit()
            logger.info(f"> ID={car_id} => Status diupdate ke '{status}'")
        except Exception as e:
            logger.error(f"❌ Gagal update_car_status ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def track_listings(self, start_id=1):
        conn = get_database_connection()
        if not conn:
            logger.error("Koneksi database gagal, tidak bisa memulai tracking.")
            return

        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT id, listing_url, status
            FROM {DB_TABLE_PRIMARY}
            WHERE (status = 'active' OR status = 'unknown') AND id >= %s
            ORDER BY id
        """, (start_id,))
        listings = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"📄 Total data: {len(listings)}")

        for i in range(0, len(listings), self.batch_size):
            batch = listings[i:i + self.batch_size]
            self.init_browser()
            self.get_current_ip()

            for car_id, url, current_status in batch:
                logger.info(f"🔍 Memeriksa ID={car_id} - {url}")
                try:
                    self.page.goto(url, timeout=30000)
                    time.sleep(2)
                    self.page.evaluate("window.scrollTo(0, 1000)")
                    self.random_delay()

                    not_found_selector = "h1.zeta.alert.alert--warning"
                    not_found_text = "Page not found."
                    if self.page.locator(not_found_selector).count() > 0:
                        text_found = self.page.locator(not_found_selector).first.inner_text().strip()
                        if not_found_text in text_found:
                            logger.info(f"404 ID={car_id} => Halaman tidak ditemukan, tandai UNKNOWN")
                            self.update_car_status(car_id, "unknown")
                            continue

                    if self.page.locator(self.sold_selector).count() > 0:
                        sold_text = self.page.locator(self.sold_selector).first.inner_text().strip()
                        if self.sold_text_indicator in sold_text:
                            logger.info(f"✅ ID={car_id} => Terjual (deteksi teks sold)")
                            self.update_car_status(car_id, "sold", datetime.now())
                            continue

                    if self.page.locator(self.active_selector).count() > 0:
                        logger.info(f"> ID={car_id} => Aktif (judul ditemukan)")
                        if current_status == "unknown":
                            self.update_car_status(car_id, "active")
                        continue

                    content = self.page.content().lower()
                    if self.sold_text_indicator.lower() in content:
                        logger.info(f"🕵️ ID={car_id} => Terjual (fallback by content)")
                        self.update_car_status(car_id, "sold", datetime.now())
                    else:
                        self.update_car_status(car_id, "unknown")

                except TimeoutError:
                    logger.warning(f"⚠️ Timeout ID={car_id}, tandai UNKNOWN")
                    # Screenshot agar tahu kenapa timeout
                    take_screenshot(self.page, f"timeout_{car_id}")
                    self.update_car_status(car_id, "unknown")

                except Exception as e:
                    logger.error(f"❌ Gagal ID={car_id}: {e}")
                    # Screenshot error
                    take_screenshot(self.page, f"error_{car_id}")
                    self.update_car_status(car_id, "unknown")

                self.random_delay()

            self.quit_browser()

        logger.info("✅ Proses tracking selesai.")