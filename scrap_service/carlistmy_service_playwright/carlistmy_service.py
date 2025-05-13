import os
import re
import random
import time
import logging
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

from .database import get_connection

load_dotenv()

START_DATE = datetime.now().strftime('%Y%m%d')

# ===== Konfigurasi Env
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history")
DB_TABLE_HISTORY_PRICE_COMBINED = os.getenv("DB_TABLE_HISTORY_PRICE_COMBINED", "price_history_combined")
INPUT_FILE = os.getenv("INPUT_FILE")

USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USERNAME = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# ===== Konfigurasi Logging
log_dir = Path(__file__).resolve().parents[2] /  "logs"
log_dir.mkdir(parents=True, exist_ok=True)

# Nama file log menggunakan tanggal saat *pertama kali program dijalankan*
log_file = log_dir / f"scrape_carlistmy_{START_DATE}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def take_screenshot(page, name: str):
    """
    Simpan screenshot ke dalam folder "scraping/logs/<YYYYMMDD>_error/"
    Di mana <YYYYMMDD> adalah tanggal screenshot diambil (bukan START_DATE).
    """
    try:
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_carlistmy"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"

        page.screenshot(path=str(screenshot_path))
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")

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
            logging.warning(f"Format proxy tidak valid: {p}")
    return parsed

class CarlistMyService:
    def __init__(self):
        self.stop_flag = False
        self.batch_size = 25
        self.listing_count = 0
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))

    def build_proxy_config(self):
        proxy_mode = os.getenv("PROXY_MODE", "none").lower()

        if proxy_mode == "oxylabs":
            username_base = os.getenv("PROXY_USERNAME", "")
            proxy_config = {
                "server": os.getenv("PROXY_SERVER"),
                "username": f"{username_base}-sessid-{self.session_id}",
                "password": os.getenv("PROXY_PASSWORD")
            }
            logging.info(f"🌐 Proxy Oxylabs dengan session: {self.session_id}")
            return proxy_config

        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            logging.info(f"🌐 Proxy custom digunakan: {proxy['server']}")
            return proxy

        else:
            logging.info("⚡ Menjalankan tanpa proxy")
            return None

    def init_browser(self):
        self.playwright = sync_playwright().start()

        launch_kwargs = {
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security"
            ]
        }

        proxy_config = self.build_proxy_config()
        if proxy_config:
            launch_kwargs["proxy"] = proxy_config

        self.browser = self.playwright.chromium.launch(**launch_kwargs)

        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur",
            geolocation={"longitude": 101.68627540160966, "latitude": 3.1504925396418315},
            permissions=["geolocation"],
            viewport={"width": 1920, "height": 1080},  # Set to full page size
        )

        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi dengan stealth.")


    def detect_anti_bot(self):
        content = self.page.content()
        if "Checking your browser before accessing" in content or "cf-browser-verification" in content:
            take_screenshot(self.page, "cloudflare_block")
            logging.warning("⚠️ Terkena anti-bot Cloudflare. Akan ganti proxy dan retry...")
            return True
        return False

    def retry_with_new_proxy(self):
        logging.info("🔁 Mengganti session proxy dan reinit browser...")
        self.session_id = self.generate_session_id()
        self.quit_browser()
        self.init_browser()
        try:
            self.get_current_ip()
        except Exception as e:
            logging.warning(f"Gagal get IP: {e}")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception as e:
            logging.error(e)
        self.playwright.stop()
        logging.info("🛑 Browser Playwright ditutup.")

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

    def scrape_detail(self, url):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.page.goto(url, wait_until="networkidle", timeout=60000)
                time.sleep(7)

                # Cek jika halaman diblokir Cloudflare
                page_title = self.page.title()
                if page_title.strip() == "Just a moment...":
                    logging.warning("🛑 Halaman diblokir Cloudflare. Mengganti proxy dan retry...")
                    take_screenshot(self.page, "cloudflare_detected")
                    retry_count += 1
                    self.retry_with_new_proxy()
                    continue  # Coba ulang URL yang sama
                else:
                    # Lanjutkan parsing HTML
                    soup = BeautifulSoup(self.page.content(), "html.parser")

                    def extract(selector):
                        element = soup.select_one(selector)
                        return element.text.strip() if element else None

                    brand = extract("#listing-detail li:nth-child(3) > a > span")
                    model = extract("#listing-detail li:nth-child(4) > a > span")
                    variant = extract("#listing-detail li:nth-child(5) > a > span")
                    informasi_iklan = extract("div:nth-child(1) > span.u-color-muted")
                    lokasi_part1 = extract("div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(2)")
                    lokasi_part2 = extract("div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(3)")
                    lokasi = " - ".join(filter(None, [lokasi_part1, lokasi_part2]))

                    price_string = extract("div.listing__item-price > h3")
                    year = extract("div.owl-stage div:nth-child(2) span.u-text-bold")
                    millage = extract("div.owl-stage div:nth-child(3) span.u-text-bold")
                    transmission = extract("div.owl-stage div:nth-child(6) span.u-text-bold")
                    seat_capacity = extract("div.owl-stage div:nth-child(7) span.u-text-bold")

                    img_tags = soup.select("#details-gallery img")
                    gambar = [img.get("src") for img in img_tags if img.get("src")]

                    price = int(re.sub(r"[^\d]", "", price_string)) if price_string else 0
                    year_int = int(re.search(r"\d{4}", year).group()) if year else 0

                    return {
                        "listing_url": url,
                        "brand": brand,
                        "model": model,
                        "variant": variant,
                        "informasi_iklan": informasi_iklan,
                        "lokasi": lokasi,
                        "price": price,
                        "year": year_int,
                        "millage": millage,
                        "transmission": transmission,
                        "seat_capacity": seat_capacity,
                        "gambar": gambar,
                    }

            except Exception as e:
                logging.error(f"Gagal scraping detail {url}: {e}")
                take_screenshot(self.page, "scrape_detail_error")
                retry_count += 1
                self.retry_with_new_proxy()

        logging.error(f"❌ Gagal mengambil data dari {url} setelah {max_retries} percobaan.")
        return None

    def save_to_db(self, car):
        try:
            self.cursor.execute(f"SELECT id, price, version FROM {DB_TABLE_SCRAP} WHERE listing_url = %s", (car["listing_url"],))
            row = self.cursor.fetchone()
            now = datetime.now()

            if row:
                car_id, old_price, version = row
                if car["price"] != old_price:
                    self.cursor.execute(f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """, (car_id, old_price, car["price"]))

                self.cursor.execute(f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model=%s, variant=%s, informasi_iklan=%s,
                        lokasi=%s, price=%s, year=%s, millage=%s,
                        transmission=%s, seat_capacity=%s, gambar=%s,
                        last_scraped_at=%s, version=%s
                    WHERE id=%s
                """, (
                    car.get("brand"), car.get("model"), car.get("variant"), car.get("informasi_iklan"),
                    car.get("lokasi"), car.get("price"), car.get("year"), car.get("millage"),
                    car.get("transmission"), car.get("seat_capacity"), car.get("gambar"),
                    now, version + 1, car_id
                ))
            else:
                self.cursor.execute(f"""
                    INSERT INTO {DB_TABLE_SCRAP} (
                        listing_url, brand, model, variant, informasi_iklan, lokasi,
                        price, year, millage, transmission, seat_capacity, gambar, version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    car["listing_url"], car.get("brand"), car.get("model"), car.get("variant"),
                    car.get("informasi_iklan"), car.get("lokasi"), car.get("price"),
                    car.get("year"), car.get("millage"), car.get("transmission"),
                    car.get("seat_capacity"), car.get("gambar"), 1
                ))

            self.conn.commit()
            logging.info(f"✅ Data untuk {car['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan ke database: {e}")

    def scrape_all_brands(self, start_brand=None, start_page=1):
        self.reset_scraping()
        df = pd.read_csv(INPUT_FILE)

        if start_brand:
            start_scraping = False
        else:
            start_scraping = True

        for _, row in df.iterrows():
            brand = row["brand"]
            base_url = row["url"]

            if not start_scraping:
                if brand.lower() == start_brand.lower():
                    start_scraping = True
                else:
                    continue

            if start_brand and brand.lower() == start_brand.lower():
                current_page = start_page
            else:
                current_page = 1

            logging.info(f"🚀 Mulai scraping brand: {brand} dengan start_page={current_page}")
            self.init_browser()

            try:
                self.get_current_ip()
            except Exception as e:
                logging.warning(f"Gagal get IP: {e}")

            page = current_page
            while not self.stop_flag:
                paginated_url = re.sub(r"(page_number=)\d+", lambda m: f"{m.group(1)}{page}", base_url)
                logging.info(f"📄 Scraping halaman {page}: {paginated_url}")

                try:
                    self.page.goto(paginated_url, timeout=60000)
                except Exception as e:
                    logging.warning(f"❌ Gagal memuat halaman {paginated_url}: {e}")
                    take_screenshot(self.page, f"page_load_error_{brand}_{page}")
                    break

                html = self.page.content()
                soup = BeautifulSoup(html, "html.parser")
                link_tags = soup.select("a.ellipsize.js-ellipsize-text")

                urls = []
                for tag in link_tags:
                    href = tag.get("href")
                    if href and "carlist.my" in href:
                        urls.append(href)
                urls = list(set(urls))
                logging.info(f"📄 Ditemukan {len(urls)} listing URL di halaman {page}")

                if not urls:
                    logging.warning(f"📄 Ditemukan 0 listing URL di halaman {page}")
                    take_screenshot(self.page, f"no_listing_page{page}_{brand}")
                    break

                logging.info("⏳ Menunggu selama 15-30 detik sebelum melanjutkan...")
                time.sleep(random.uniform(17, 39))

                for url in urls:
                    if self.stop_flag:
                        break
                    logging.info(f"🔍 Scraping detail: {url}")
                    detail = self.scrape_detail(url)
                    if detail:
                        self.save_to_db(detail)
                        self.listing_count += 1
                        time.sleep(random.uniform(20, 40))  # Tunggu selama 15-20 detik secara acak antara detail

                        if self.listing_count >= self.batch_size:
                            self.quit_browser()
                            time.sleep(5)
                            self.init_browser()
                            try:
                                self.get_current_ip()
                            except Exception as e:
                                logging.warning(f"Gagal get IP: {e}")
                            self.listing_count = 0

                page += 1
                time.sleep(random.uniform(5, 10))

            self.quit_browser()

        logging.info("✅ Semua brand telah selesai diproses.")

    def sync_to_cars(self):
        """
        Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}, d
        an sinkronisasi perubahan harga dari price_history ke price_history_combined.
        """
        logging.info(f"Memulai sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}...")
        try:
            # Sinkronisasi data dari cars_scrap ke cars (update atau insert data mobil)
            fetch_query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(fetch_query)
            rows = self.cursor.fetchall()
            col_names = [desc[0] for desc in self.cursor.description]
            idx_url = col_names.index("listing_url")

            for row in rows:
                listing_url = row[idx_url]
                check_query = f"SELECT id FROM {DB_TABLE_PRIMARY} WHERE listing_url = %s"
                self.cursor.execute(check_query, (listing_url,))
                result = self.cursor.fetchone()

                if result:
                    update_query = f"""
                        UPDATE {DB_TABLE_PRIMARY}
                        SET brand=%s, model=%s, variant=%s, informasi_iklan=%s,
                            lokasi=%s, price=%s, year=%s, millage=%s, transmission=%s,
                            seat_capacity=%s, gambar=%s, last_scraped_at=%s
                        WHERE listing_url=%s
                    """
                    self.cursor.execute(update_query, (
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("informasi_iklan")],
                        row[col_names.index("lokasi")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("millage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("gambar")],
                        row[col_names.index("last_scraped_at")],
                        listing_url
                    ))
                else:
                    insert_query = f"""
                        INSERT INTO {DB_TABLE_PRIMARY}
                            (listing_url, brand, model, variant, informasi_iklan, lokasi,
                             price, year, millage, transmission, seat_capacity, gambar, last_scraped_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        listing_url,
                        row[col_names.index("brand")],
                        row[col_names.index("model")],
                        row[col_names.index("variant")],
                        row[col_names.index("informasi_iklan")],
                        row[col_names.index("lokasi")],
                        row[col_names.index("price")],
                        row[col_names.index("year")],
                        row[col_names.index("millage")],
                        row[col_names.index("transmission")],
                        row[col_names.index("seat_capacity")],
                        row[col_names.index("gambar")],
                        row[col_names.index("last_scraped_at")]
                    ))

            # Sinkronisasi perubahan harga dari price_history ke price_history_combined
            sync_price_history_query = f"""
                INSERT INTO {DB_TABLE_HISTORY_PRICE_COMBINED} (car_id, car_scrap_id, old_price, new_price, changed_at)
                SELECT c.id, cs.id, ph.old_price, ph.new_price, ph.changed_at
                FROM {DB_TABLE_HISTORY_PRICE} ph
                JOIN {DB_TABLE_SCRAP} cs ON ph.car_id = cs.id
                JOIN {DB_TABLE_PRIMARY} c ON cs.listing_url = c.listing_url
                WHERE ph.car_id IS NOT NULL;
            """
            self.cursor.execute(sync_price_history_query)

            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
            logging.info("Sinkronisasi perubahan harga dari price_history ke price_history_combined selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error saat sinkronisasi data: {e}")

    def export_data(self):
        try:
            self.cursor.execute(f"SELECT * FROM {DB_TABLE_SCRAP}")
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logging.error(f"❌ Error export data: {e}")
            return []

    def stop_scraping(self):
        self.stop_flag = True
        logging.info("🛑 Scraping dihentikan oleh user.")

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

    def close(self):
        try:
            self.quit_browser()
        except:
            pass
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logging.error(f"❌ Error saat close koneksi: {e}")
