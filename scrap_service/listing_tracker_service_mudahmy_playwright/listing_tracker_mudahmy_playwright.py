import os
import time
import random
import logging
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
from playwright_stealth import stealth_sync
from pathlib import Path

from scrap_service.listing_tracker_service_mudahmy_playwright.database import get_database_connection

load_dotenv()

START_DATE = datetime.now().strftime('%Y%m%d')

base_dir = Path(__file__).resolve().parents[2]
log_dir = base_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"tracker_mudahmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("tracker")

def take_screenshot(page, name: str):
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_mudahmy_tracker"
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
        os.getenv("USE_PROXY", "false").lower() == "true" and
        os.getenv("PROXY_SERVER") and
        os.getenv("PROXY_USERNAME") and
        os.getenv("PROXY_PASSWORD")
    )

class ListingTrackerMudahmyPlaywright:
    def __init__(self, batch_size=25):
        self.batch_size = batch_size
        self.redirect_url = "https://www.mudah.my/malaysia/cars-for-sale"
        self.active_selector = "#ad_view_ad_highlights h1"
        self.sold_text_indicator = "This car has already been sold."
        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0
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
            "headless": True,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        }

        proxy = self.build_proxy_config()
        if proxy:
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy digunakan: {proxy['server']}")
        else:
            logging.info("⚡ Browser tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur",
            geolocation={"longitude": 101.68627540160966, "latitude": 3.1504925396418315},
            permissions=["geolocation"]
        )

        self.context.route("**/*", lambda route,
                                          request: route.abort() if request.resource_type == "image" else route.continue_())

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def detect_anti_bot(self):
        try:
            content = self.page.content()
            if "Checking your browser before accessing" in content or "cf-browser-verification" in content or "Server Error" in content:
                take_screenshot(self.page, "cloudflare_block")
                logging.warning("⚠️ Terkena proteksi anti-bot. Akan ganti proxy dan retry...")
                return True
            return False
        except Exception as e:
            logging.warning(f"❌ Gagal mendeteksi anti-bot: {e}")
            return False

    def retry_with_new_proxy(self):
        self.quit_browser()
        self.session_id = self.generate_session_id()
        self.init_browser()
        self.get_current_ip()
        logging.info("🔁 Browser reinit dengan session proxy baru.")

    def get_current_ip(self, retries=3):
        for attempt in range(retries):
            try:
                self.page.goto("https://ip.oxylabs.io/", timeout=10000)
                ip = self.page.inner_text("body").strip()
                logging.info(f"🌐 IP yang digunakan: {ip}")
                return ip
            except Exception as e:
                logging.warning(f"Gagal mengambil IP (percobaan {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(7)
        raise Exception("Gagal mengambil IP setelah beberapa retry.")

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

    def random_delay(self, min_d=5, max_d=7):
        time.sleep(random.uniform(min_d, max_d))

    def is_redirected(self, title, url):
        return "Cars For Sale In Malaysia" in title or "/cars-for-sale" in url

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
            logger.error(f"❌ Error update_car_status untuk ID={car_id}: {e}")
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
            batch = listings[i:i+self.batch_size]
            self.init_browser()
            self.get_current_ip()

            for car_id, url, current_status in batch:
                logger.info(f"🔍 Memeriksa ID={car_id} - {url}")
                try:
                    self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(5)
                    if self.detect_anti_bot():
                        self.retry_with_new_proxy()
                        self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        time.sleep(2)

                    self.page.evaluate("window.scrollTo(0, 1000)")
                    self.random_delay()

                    title = self.page.title()
                    current_url = self.page.url
                    logger.info(f"🔎 URL: {current_url}")
                    logger.info(f"🔎 Title: {title}")

                    # Cek redirect
                    if self.is_redirected(title, current_url):
                        self.update_car_status(car_id, "sold", datetime.now())
                        continue

                    # Cek element #ad_view_ad_highlights h1 => menandakan masih aktif
                    if self.page.locator(self.active_selector).count() > 0:
                        logger.info(f"> ID={car_id} => Aktif (H1 ditemukan)")
                        if current_status == "unknown":
                            self.update_car_status(car_id, "active")
                    else:
                        # Cek teks "This car has already been sold." di page content
                        content = self.page.content().lower()
                        if self.sold_text_indicator.lower() in content:
                            self.update_car_status(car_id, "sold", datetime.now())
                        else:
                            self.update_car_status(car_id, "unknown")

                except TimeoutError:
                    logger.warning(f"⚠️ Timeout saat memeriksa ID={car_id}. Coba cek redirect secara manual...")
                    # Ambil info via evaluate
                    try:
                        current_url = self.page.evaluate("() => window.location.href")
                        title = self.page.evaluate("() => document.title")
                        logger.info(f"🔎 [Fallback JS] URL = {current_url}")
                        logger.info(f"🔎 [Fallback JS] Title = {title}")
                        if self.is_redirected(title, current_url):
                            logger.info(f"🔁 ID={car_id} => Redirect terdeteksi. Tandai sebagai SOLD.")
                            self.update_car_status(car_id, "sold", datetime.now())
                        else:
                            logger.info(f"🔁 ID={car_id} => Tidak redirect. Tandai sebagai UNKNOWN.")
                            self.update_car_status(car_id, "unknown")
                    except Exception as inner:
                        logger.error(f"❌ Gagal fallback setelah timeout: {inner}")
                        # Ambil screenshot
                        take_screenshot(self.page, f"timeout_fallback_{car_id}")
                        self.update_car_status(car_id, "unknown")

                except Exception as e:
                    logger.error(f"❌ Gagal memeriksa ID={car_id}: {e}")
                    # Screenshot error
                    take_screenshot(self.page, f"error_{car_id}")
                    self.update_car_status(car_id, "unknown")

                self.random_delay()

            self.quit_browser()

        logger.info("✅ Proses tracking selesai.")
