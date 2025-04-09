import os
import time
import random
import logging
import re
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from .database import get_connection
from pathlib import Path

load_dotenv()


START_DATE = datetime.now().strftime('%Y%m%d')


# ================== Konfigurasi ENV
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "url")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history_scrap")
DB_TABLE_HISTORY_PRICE_COMBINED = os.getenv("DB_TABLE_HISTORY_PRICE_COMBINED", "price_history_combined")
INPUT_FILE = os.getenv("INPUT_FILE", "mudahmy_service_playwright/storage/inputfiles/mudahMY_scraplist.csv")


# ================== Konfigurasi PATH Logging
base_dir = Path(__file__).resolve().parents[2]   # <--- 2 level di atas file ini
log_dir = base_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / f"scrape_mudahmy_{START_DATE}.log"

# ================== Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def take_screenshot(page, name):
    try:
        # Folder error sesuai TANGGAL sekarang (bisa beda dari START_DATE)
        error_folder_name = datetime.now().strftime('%Y%m%d') + "_error_mudahmy"
        screenshot_dir = log_dir / error_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%H%M%S')
        screenshot_path = screenshot_dir / f"{name}_{timestamp}.png"

        page.screenshot(path=str(screenshot_path))
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")


def should_use_proxy():
    return (
        os.getenv("USE_PROXY", "false").lower() == "true" and
        os.getenv("PROXY_SERVER") and
        os.getenv("PROXY_USERNAME") and
        os.getenv("PROXY_PASSWORD")
    )


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

class MudahMyService:
    def __init__(self):
        self.stop_flag = False
        self.batch_size = 40
        self.listing_count = 0

        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        self.custom_proxies = get_custom_proxy_list()
        self.proxy_index = 0

    def init_browser(self):
        self.playwright = sync_playwright().start()
        launch_kwargs = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security"
            ]
        }

        proxy_mode = os.getenv("PROXY_MODE", "none").lower()
        if proxy_mode == "oxylabs":
            launch_kwargs["proxy"] = {
                "server": os.getenv("PROXY_SERVER"),
                "username": os.getenv("PROXY_USERNAME"),
                "password": os.getenv("PROXY_PASSWORD")
            }
            logging.info("🌐 Proxy aktif (Oxylabs digunakan)")
        elif proxy_mode == "custom" and self.custom_proxies:
            proxy = random.choice(self.custom_proxies)
            launch_kwargs["proxy"] = proxy
            logging.info(f"🌐 Proxy custom digunakan (random): {proxy['server']}")
        else:
            logging.info("⚡ Menjalankan browser tanpa proxy")

        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur"
        )
        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("✅ Browser Playwright berhasil diinisialisasi.")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception as e:
            logging.error(e)
        self.playwright.stop()
        logging.info("🛑 Browser Playwright ditutup.")

    def get_current_ip(self, page, retries=3):
        """Contoh memanggil ip.oxylabs.io untuk cek IP."""
        for attempt in range(1, retries + 1):
            try:
                page.goto('https://ip.oxylabs.io/', timeout=10000)
                ip_text = page.inner_text('body')
                ip = ip_text.strip()
                logging.info(f"IP Saat Ini: {ip}")
                return
            except Exception as e:
                logging.warning(f"Attempt {attempt} gagal mendapatkan IP: {e}")
                # Jika gagal screenshot pun
                take_screenshot(page, "failed_get_ip")
                if attempt == retries:
                    logging.error("Gagal mendapatkan IP setelah beberapa percoaan")
                else:
                    time.sleep(3)

    def scrape_page(self, page, url):
        """
        Scrape satu halaman (listing) dan kembalikan daftar URL detail.
        Jika gagal, kembalikan list kosong.
        """
        try:
            self.get_current_ip(page)
            delay = random.uniform(3, 7)
            logging.info(f"Menuju {url} (delay {delay:.1f}s)")
            time.sleep(delay)
            page.goto(url, timeout=60000)

            # Contoh deteksi blocked
            if page.locator("text='Access Denied'").is_visible(timeout=3000):
                raise Exception("Akses ditolak")
            if page.locator("text='Please verify you are human'").is_visible(timeout=3000):
                take_screenshot(page, "captcha_detected")
                raise Exception("Deteksi CAPTCHA")

            page.wait_for_load_state('networkidle', timeout=15000)

            # Coba beberapa selector
            selectors = [
                ('css', 'div.flex.flex-col.flex-1.gap-2.self-center div.flex.flex-col a'),
                ('xpath', '//a[contains(@href,"mudah.my") and contains(@class,"sc-jwKygS")]'),
                ('xpath', '//div[contains(@class,"listing-item")]//a[contains(@href,"mudah.my")]')
            ]

            listings = []
            for strategy, selector in selectors:
                try:
                    if strategy == 'css':
                        elements = page.query_selector_all(selector)
                    else:
                        elements = page.query_selector_all(f'{strategy}={selector}')
                    if elements:
                        listings = elements
                        break
                except Exception as e:
                    logging.warning(f"Selector {selector} gagal: {e}")
                    continue

            if not listings:
                take_screenshot(page, "no_listings_found")
                logging.warning("Tidak menemukan listing dengan semua selector")
                return []

            urls = []
            for element in listings:
                href = element.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        href = urljoin(url, href)
                    if 'mudah.my' in href:
                        urls.append(href)

            total_listing = len(list(set(urls)))
            logging.info(f"📄 Ditemukan {total_listing} listing URLs di halaman {url}.")
            return list(set(urls))

        except Exception as e:
            logging.error(f"Error saat scraping halaman: {e}")
            take_screenshot(page, f"error_scrape_page")
            return []

    def scrape_listing_detail(self, page, url):
        """Scrape detail listing. Kembalikan dict data, atau None kalau gagal."""
        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            try:
                logging.info(f"Navigating to detail page: {url} (Attempt {attempt+1})")
                page.goto(url, wait_until="networkidle", timeout=120000)

                if "Access Denied" in page.title() or "block" in page.url:
                    raise Exception("Blocked by anti-bot protection")

                def safe_extract(selectors, selector_type="css", fallback="N/A"):
                    for selector in selectors:
                        try:
                            if selector_type == "css":
                                if page.locator(selector).count() > 0:
                                    return page.locator(selector).first.inner_text().strip()
                            elif selector_type == "xpath":
                                xp = f"xpath={selector}"
                                if page.locator(xp).count() > 0:
                                    return page.locator(xp).first.inner_text().strip()
                        except Exception as e:
                            logging.warning(f"Selector failed: {selector} - {e}")
                    return fallback

                data = {}
                data["listing_url"] = url
                data["brand"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(1) > div:nth-child(3)",
                    "div:has-text('Brand') + div",
                    "//div[contains(text(),'Brand')]/following-sibling::div"
                ])
                data["model"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(2) > div:nth-child(3)",
                    "div:has-text('Model') + div",
                    "//div[contains(text(),'Model')]/following-sibling::div"
                ])
                data["variant"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(4) > div:nth-child(3)",
                    "div:has-text('Variant') + div",
                    "//span[contains(text(),'Variant')]/following-sibling::span"
                ])
                data["informasi_iklan"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div > div",
                    "div.ad-highlight:first-child",
                    "//div[contains(@class,'ad-highlight')][1]"
                ])
                data["lokasi"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(4) > div",
                    "div:has-text('Location') + div",
                    "//div[contains(text(),'Location')]/following-sibling::div"
                ])
                data["price"] = safe_extract([
                    "div.flex.gap-1.md\\:items-end > div"
                ])
                data["year"] = safe_extract([
                    "#ad_view_car_specifications div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Year') + div",
                    "//div[contains(text(),'Year')]/following-sibling::div"
                ])
                data["millage"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(3) > div",
                    "div:has-text('Mileage') + div",
                    "//div[contains(text(),'Mileage')]"
                ])
                data["transmission"] = safe_extract([
                    "#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(2) > div",
                    "div:has-text('Transmission') + div",
                    "//div[contains(text(),'Transmission')]"
                ])
                data["seat_capacity"] = safe_extract([
                    "#ad_view_car_specifications > div > div > div > div > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3)",
                    "div:has-text('Seat Capacity') + div",
                    "//div[contains(text(),'Seat') and contains(text(),'Capacity')]"
                ])
                images = page.evaluate("""() => {
                    const gallery = document.getElementById('ad_view_gallery');
                    if (!gallery) return [];
                    return Array.from(gallery.querySelectorAll('img')).map(img => img.src);
                }""")
                data["gambar"] = images
                data["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                return data

            except Exception as e:
                logging.error(f"Scraping detail failed: {e}")
                take_screenshot(page, f"error_scrape_detail")
                attempt += 1
                if attempt < max_retries:
                    logging.warning(f"Mencoba ulang detail scraping untuk {url} (Attempt {attempt+1})...")
                    time.sleep(random.uniform(5, 10))
                else:
                    logging.warning(f"Gagal mengambil detail untuk URL: {url}")
                    return None

    def scrape_listings_for_brand(self, base_url, brand_name, model_name, start_page=1):
        total_scraped = 0
        current_page = start_page
        self.init_browser()
        try:
            while True:
                if self.stop_flag:
                    logging.info("Stop flag terdeteksi, menghentikan scraping brand ini.")
                    break

                current_url = f"{base_url}?o={current_page}"
                logging.info(f"Scraping halaman {current_page}: {current_url}")
                listing_urls = self.scrape_page(self.page, current_url)

                if not listing_urls:
                    logging.info("Tidak ada listing URL ditemukan, pindah ke brand/model berikutnya.")
                    break

                for url in listing_urls:
                    if self.stop_flag:
                        break

                    detail_data = self.scrape_listing_detail(self.page, url)
                    if detail_data:
                        max_db_retries = 3
                        for attempt in range(1, max_db_retries + 1):
                            try:
                                self.save_to_db(detail_data)
                                break
                            except Exception as e:
                                logging.warning(f"⚠️ Attempt {attempt} gagal simpan data untuk {url}: {e}")
                                if attempt == max_db_retries:
                                    logging.error(f"❌ Gagal simpan data setelah {max_db_retries} percobaan: {url}")
                                else:
                                    time.sleep(2)
                        total_scraped += 1
                    else:
                        logging.warning(f"Gagal mengambil detail untuk URL: {url}")

                    delay = random.uniform(2, 5)
                    logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                    time.sleep(delay)

                    # Re-init browser setiap kali batch_size tercapai
                    if total_scraped % self.batch_size == 0 and total_scraped != 0:
                        logging.info(f"Batch {self.batch_size} listing tercapai, reinit browser.")
                        self.quit_browser()
                        time.sleep(3)
                        self.init_browser()

                current_page += 1
                delay = random.uniform(5, 10)
                logging.info(f"Menunggu {delay:.1f} detik sebelum halaman berikutnya...")
                time.sleep(delay)

            logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")
        finally:
            self.quit_browser()
        return total_scraped, False

    def scrape_all_brands(self, brand=None, model=None, start_page=1):
        """
        Baca CSV:
          - Jika brand dan model diberikan, mulai scraping dari baris yang cocok,
            lalu lanjut ke seluruh baris berikutnya.
          - Jika tidak diberikan, scraping semua brand+model (dari baris pertama).
        """
        self.reset_scraping()
        df = pd.read_csv(INPUT_FILE)

        if brand and model:
            df = df.reset_index(drop=True)
            matching_rows = df[
                (df['brand'].str.lower() == brand.lower()) &
                (df['model'].str.lower() == model.lower())
            ]
            if matching_rows.empty:
                logging.warning("Brand dan model tidak ditemukan dalam CSV.")
                return

            start_index = matching_rows.index[0]
            logging.info(f"Mulai scraping dari baris {start_index} untuk brand={brand}, model={model} (start_page={start_page}).")

            for i in range(start_index, len(df)):
                row = df.iloc[i]
                brand_name = row['brand']
                model_name = row['model']
                base_url = row['url']

                if i == start_index:
                    current_page = start_page
                else:
                    current_page = 1

                logging.info(f"Mulai scraping brand: {brand_name}, model: {model_name}, start_page={current_page}")
                total_scraped, _ = self.scrape_listings_for_brand(base_url, brand_name, model_name, current_page)
                logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")
        else:
            logging.info("Mulai scraping dari baris pertama (tidak ada filter brand/model).")
            df = df.reset_index(drop=True)
            for i, row in df.iterrows():
                brand_name = row['brand']
                model_name = row['model']
                base_url = row['url']

                logging.info(f"Mulai scraping brand: {brand_name}, model: {model_name}, start_page=1")
                total_scraped, _ = self.scrape_listings_for_brand(base_url, brand_name, model_name, 1)
                logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")

        logging.info("Proses scraping selesai untuk filter brand/model.")

    def stop_scraping(self):
        logging.info("Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("Scraping direset.")

    def save_to_db(self, car_data):
        """
        Simpan atau update data mobil ke database.
        """
        try:
            self.cursor.execute(
                f"SELECT id, price, version FROM {DB_TABLE_SCRAP} WHERE listing_url = %s",
                (car_data["listing_url"],)
            )
            row = self.cursor.fetchone()

            # Normalisasi price -> integer
            price_int = 0
            if car_data.get("price"):
                match_price = re.sub(r"[^\d]", "", car_data["price"])  # buang non-digit
                price_int = int(match_price) if match_price else 0

            # Normalisasi year -> integer
            year_int = 0
            if car_data.get("year"):
                match_year = re.search(r"(\d{4})", car_data["year"])
                if match_year:
                    year_int = int(match_year.group(1))

            if row:
                car_id, old_price, current_version = row
                old_price = old_price if old_price else 0
                current_version = current_version if current_version else 0
                new_price = price_int

                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand=%s, model=%s, variant=%s,
                        informasi_iklan=%s, lokasi=%s,
                        price=%s, year=%s, millage=%s,
                        transmission=%s, seat_capacity=%s,
                        gambar=%s, last_scraped_at=%s,
                        version=%s
                    WHERE id=%s
                """

                new_version = current_version + 1
                self.cursor.execute(update_query, (
                    car_data.get("brand"),
                    car_data.get("model"),
                    car_data.get("variant"),
                    car_data.get("informasi_iklan"),
                    car_data.get("lokasi"),
                    new_price,
                    year_int,
                    car_data.get("millage"),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("gambar"),
                    datetime.now(),
                    new_version,
                    car_id
                ))

                # Jika harga berubah, catat di history
                if new_price != old_price and old_price != 0:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_id, old_price, new_price))

            else:
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                        (listing_url, brand, model, variant, informasi_iklan, lokasi,
                         price, year, millage, transmission, seat_capacity, gambar, version)
                    VALUES
                        (%s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, (
                    car_data["listing_url"],
                    car_data.get("brand"),
                    car_data.get("model"),
                    car_data.get("variant"),
                    car_data.get("informasi_iklan"),
                    car_data.get("lokasi"),
                    price_int,
                    year_int,
                    car_data.get("millage"),
                    car_data.get("transmission"),
                    car_data.get("seat_capacity"),
                    car_data.get("gambar"),
                    1
                ))

            self.conn.commit()
            logging.info(f"✅ Data untuk listing_url={car_data['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def sync_to_cars(self):
        """
        Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}, dan sinkronisasi data perubahan harga dari price_history_scrap ke price_history_combined.
        """
        logging.info(f"Memulai sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}...")
        try:
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

            # Sinkronisasi perubahan harga dari price_history_scrap ke price_history_combined
            sync_price_history_query = f"""
                INSERT INTO {DB_TABLE_HISTORY_PRICE_COMBINED} (car_id, car_scrap_id, old_price, new_price, changed_at)
                SELECT c.id, cs.id, phs.old_price, phs.new_price, phs.changed_at
                FROM {DB_TABLE_HISTORY_PRICE} phs
                JOIN {DB_TABLE_SCRAP} cs ON phs.car_id = cs.id
                JOIN {DB_TABLE_PRIMARY} c ON cs.listing_url = c.listing_url
                WHERE phs.car_id IS NOT NULL;
            """
            self.cursor.execute(sync_price_history_query)

            # Commit perubahan ke database
            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
            logging.info("Sinkronisasi perubahan harga dari price_history_scrap ke price_history_combined selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error saat sinkronisasi data: {e}")

    def export_data(self):
        """
        Mengambil data dari DB_TABLE_SCRAP dalam bentuk list of dict
        """
        try:
            query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            return data
        except Exception as e:
            logging.error(f"Error export data: {e}")
            return []

    def close(self):
        """Tutup browser dan koneksi database."""
        try:
            self.quit_browser()
        except Exception:
            pass
        try:
            self.cursor.close()
            self.conn.close()
            logging.info("Koneksi database ditutup, browser ditutup.")
        except Exception as e:
            logging.error(e)
