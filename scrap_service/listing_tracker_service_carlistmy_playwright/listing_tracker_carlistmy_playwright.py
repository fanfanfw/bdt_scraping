import os
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path
import sys

from scrap_service.listing_tracker_service_carlistmy_playwright.database import get_database_connection

load_dotenv()

START_DATE = datetime.now().strftime('%Y%m%d')

base_dir = Path(__file__).resolve().parents[2]
log_dir = base_dir / "logs"
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
    def __init__(self, listings_per_batch=5):
        self.listings_per_batch = listings_per_batch
        self.sold_selector = "h2"
        self.sold_text_indicator = "This car has already been sold."
        self.active_selector = "h1"
        self.custom_proxies = get_custom_proxy_list()
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy_config(self):
        proxy_mode = os.getenv("PROXY_MODE", "none").lower()

        if proxy_mode == "oxylabs":
            username_base = os.getenv("PROXY_USERNAME", "")
            return {
                "server": os.getenv("PROXY_SERVER"),
                "username": f"{username_base}-sessid-{self.session_id}",
                "password": os.getenv("PROXY_PASSWORD")
            }

        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            return proxy

        else:
            return None

    def init_browser(self):
        self.playwright = sync_playwright().start()

        launch_kwargs = {
            "headless": False,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        }

        proxy = self.build_proxy_config()
        if proxy:
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy digunakan: {proxy['server']}")
        else:
            logging.info("⚡ Browser dijalankan tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur",
            geolocation={"longitude": 101.68627540160966, "latitude": 3.1504925396418315},
            permissions=["geolocation"]
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def detect_anti_bot(self):
        try:
            content = self.page.content()
            if ("Checking your browser before accessing" in content
                or "cf-browser-verification" in content
                or "Server Error" in content):
                take_screenshot(self.page, "cloudflare_block")
                logging.warning("⚠️ Deteksi proteksi Cloudflare atau error server. Akan ganti proxy dan retry.")
                return True
            return False
        except Exception as e:
            logging.warning(f"❌ Gagal cek anti-bot: {e}")

    def retry_with_new_proxy(self):
        self.quit_browser()
        time.sleep(random.uniform(10, 15))  # cooldown ringan
        self.session_id = self.generate_session_id()
        self.init_browser()
        self.check_current_ip()
        wait_time = random.uniform(300, 900)
        logger.info(f"🕒 Jeda {wait_time:.2f} detik setelah cek IP...")
        time.sleep(wait_time)
        logger.info("🔁 Selesai reinit.")

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

    def random_delay(self, min_d=30, max_d=60):
        """Random delay between actions with default 30-60 seconds"""
        delay = random.uniform(min_d, max_d)
        logger.info(f"⏳ Jeda selama {delay:.2f} detik...")
        time.sleep(delay)

    def check_current_ip(self):
        """Cek IP saat ini menggunakan ip.oxylabs.io"""
        try:
            self.page.goto("https://ip.oxylabs.io/", timeout=10000)
            ip = self.page.inner_text("body").strip()
            logger.info(f"🌐 IP saat ini: {ip}")
        except Exception as e:
            logger.warning(f"❌ Gagal mengecek IP: {e}")

    def update_car_status(self, car_id, status, sold_at=None):
        conn = get_database_connection()
        if not conn:
            logger.error("Tidak bisa update status, koneksi database gagal.")
            return
        cursor = conn.cursor()
        try:
            now = datetime.now()
            cursor.execute(f"""
                UPDATE {DB_TABLE_PRIMARY}
                SET status = %s,
                    sold_at = %s,
                    last_scraped_at = %s,
                    last_status_check = %s
                WHERE id = %s
            """, (status, sold_at, now, now, car_id))
            conn.commit()
            logger.info(f"> ID={car_id} => Status diupdate ke '{status}', waktu cek status diset ke {now}")
        except Exception as e:
            logger.error(f"❌ Gagal update_car_status ID={car_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def detect_cloudflare_block(self):
        try:
            title = self.page.title()
            if "Just a moment..." in title:
                take_screenshot(self.page, "cloudflare_block")
                logger.warning("⚠️ Terblokir Cloudflare, reinit browser & ganti proxy.")
                return True
            return False
        except Exception as e:
            logger.warning(f"❌ Gagal cek title: {e}")
            return False

    def track_listings(self, start_id=1, status_filter="all"):
        if status_filter not in ["all", "active", "unknown"]:
            logger.warning(f"⚠️ Status filter tidak valid: {status_filter}, fallback ke 'all'")
            status_filter = "all"

        conn = get_database_connection()
        if not conn:
            logger.error("❌ Gagal koneksi database.")
            return

        cursor = conn.cursor()
        if status_filter == "all":
            cursor.execute(f"""
                SELECT id, listing_url, status
                FROM {DB_TABLE_PRIMARY}
                WHERE id >= %s
                ORDER BY id
            """, (start_id,))
        else:
            cursor.execute(f"""
                SELECT id, listing_url, status
                FROM {DB_TABLE_PRIMARY}
                WHERE status = %s AND id >= %s
                ORDER BY id
            """, (status_filter, start_id))
        listings = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"📄 Total data: {len(listings)} | Reinit setiap {self.listings_per_batch} listing")

        self.init_browser()
        self.check_current_ip()
        time.sleep(random.uniform(15, 20))  # jeda setelah cek IP

        for index, (car_id, url, current_status) in enumerate(listings, start=1):
            logger.info(f"🔍 Memeriksa ID={car_id} ({index}/{len(listings)})")

            try:
                self.page.goto(url, wait_until="domcontentloaded", timeout=60000)

                if self.detect_cloudflare_block():
                    self.retry_with_new_proxy()
                    self.page.goto(url, wait_until="domcontentloaded", timeout=60000)

                self.random_delay(7, 13)
                self.page.evaluate("window.scrollTo(0, 1000)")
                self.random_delay(15, 20)

                if self.page.locator(self.sold_selector).count() > 0:
                    sold_text = self.page.locator(self.sold_selector).first.inner_text().strip()
                    if self.sold_text_indicator in sold_text:
                        self.update_car_status(car_id, "sold", datetime.now())
                        continue

                self.update_car_status(car_id, "active")

            except TimeoutError:
                logger.warning(f"⚠️ Timeout ID={car_id}, tandai UNKNOWN")
                take_screenshot(self.page, f"timeout_{car_id}")
                self.update_car_status(car_id, "unknown")
            except Exception as e:
                logger.error(f"❌ Gagal ID={car_id}: {e}")
                take_screenshot(self.page, f"error_{car_id}")
                self.update_car_status(car_id, "unknown")

            if index % self.listings_per_batch == 0 and index < len(listings):
                logger.info("🔄 Reinit browser & proxy setelah batch.")
                self.retry_with_new_proxy()

        self.quit_browser()
        logger.info("✅ Selesai semua listing.")

