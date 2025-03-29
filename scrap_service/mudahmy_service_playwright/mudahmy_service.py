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

load_dotenv()

# Ambil konfigurasi dari environment
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "url")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history")
INPUT_FILE = os.getenv("INPUT_FILE", "mudahmy_service_playwright/storage/inputfiles/mudahMY_scraplist.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class MudahMyService:
    def __init__(self):
        self.stop_flag = False
        self.batch_size = 40
        self.listing_count = 0
        # Inisialisasi koneksi database
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

    def init_browser(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            proxy={
                "server": os.getenv("PROXY_SERVER"),
                "username": os.getenv("PROXY_USERNAME"),
                "password": os.getenv("PROXY_PASSWORD")
            },
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-web-security"
            ]
        )
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Asia/Kuala_Lumpur"
        )
        self.page = self.context.new_page()
        stealth_sync(self.page)
        logging.info("Browser Playwright berhasil diinisialisasi.")

    def quit_browser(self):
        try:
            self.browser.close()
        except Exception as e:
            logging.error(e)
        self.playwright.stop()
        logging.info("Browser Playwright ditutup.")

    def get_current_ip(self, page):
        try:
            page.goto('https://ip.oxylabs.io/', timeout=10000)
            ip_text = page.inner_text('body')
            ip = ip_text.strip()
            logging.info(f"IP Saat Ini: {ip}")
        except Exception as e:
            logging.error(f"Gagal mendapatkan IP: {e}")

    def scrape_page(self, page, url):
        try:
            self.get_current_ip(page)
            delay = random.uniform(3, 7)
            logging.info(f"Menuju {url} (delay {delay:.1f}s)")
            time.sleep(delay)
            page.goto(url, timeout=60000)
            
            if page.locator("text='Access Denied'").is_visible(timeout=3000):
                raise Exception("Akses ditolak")
            if page.locator("text='Please verify you are human'").is_visible(timeout=3000):
                page.screenshot(path="captcha_detected.png")
                raise Exception("Deteksi CAPTCHA")
            
            page.wait_for_load_state('networkidle', timeout=15000)
            
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
                page.screenshot(path="no_listings_found.png")
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
            
            # Cek jumlah listing URL dan tampilkan di log
            total_listing = len(list(set(urls)))
            logging.info(f"📄 Ditemukan {total_listing} listing URLs di halaman {url}.")
            
            return list(set(urls))
        
        except Exception as e:
            logging.error(f"Error saat scraping halaman: {e}")
            page.screenshot(path=f"error_{datetime.now().strftime('%H%M%S')}.png")
            return []

    def scrape_listing_detail(self, page, url):
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
        need_restart = False
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
                        self.save_to_db(detail_data)
                        total_scraped += 1
                    else:
                        logging.warning(f"Gagal mengambil detail untuk URL: {url}")

                    delay = random.uniform(2, 5)
                    logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                    time.sleep(delay)

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
        return total_scraped, need_restart

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
                        self.save_to_db(detail_data)
                        total_scraped += 1
                    else:
                        logging.warning(f"Gagal mengambil detail untuk URL: {url}")

                    delay = random.uniform(2, 5)
                    logging.info(f"Menunggu {delay:.1f} detik sebelum listing berikutnya...")
                    time.sleep(delay)

                    # Jika batch tercapai, reinit browser dan lanjutkan proses scraping
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
        return total_scraped

    def scrape_all_brands(self, brand=None, model=None, start_page=1):
        """
        Baca CSV:
          - Jika brand=None & model=None, scrape semua baris CSV dari awal.
          - Jika brand ada, filter CSV by brand.
          - Jika brand & model ada, filter CSV by brand+model.
          - Mulai dari halaman 'start_page' (default=1).
        """
        self.reset_scraping()

        # Baca CSV
        df = pd.read_csv(INPUT_FILE)

        # Filter berdasarkan brand & model jika ada
        if brand:
            df = df[df['brand'] == brand]
            if model:
                df = df[df['model'] == model]

        if df.empty:
            logging.warning("CSV kosong atau brand/model tidak ditemukan.")
            return

        # Loop setiap baris (brand+model) yang lolos filter
        for _, row in df.iterrows():
            brand_name = row['brand']
            model_name = row['model']
            base_url = row['url']

            logging.info(f"Mulai scraping brand: {brand_name}, model: {model_name}, start_page={start_page}")
            total_scraped, need_restart = self.scrape_listings_for_brand(base_url, brand_name, model_name, start_page)
            logging.info(f"Selesai scraping {brand_name} {model_name}. Total data: {total_scraped}")

            if brand and model:
                break

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
        Untuk kolom gambar, kirimkan list Python agar psycopg2 mengonversinya ke TEXT[].
        """
        try:
            # Cek apakah listing_url sudah ada
            self.cursor.execute("SELECT id, price FROM {} WHERE listing_url = %s".format(DB_TABLE_SCRAP), (car_data["listing_url"],))
            row = self.cursor.fetchone()

            # Konversi harga dan tahun jika diperlukan (misal 'RM 36,400' → 36400)
            price_int = int(re.sub(r"[^\d]", "", car_data.get("price", ""))) if car_data.get("price") else 0
            year_int = int(re.search(r"(\d{4})", car_data.get("year", "")).group(1)) if car_data.get("year") and re.search(r"(\d{4})", car_data.get("year", "")) else 0

            if row:
                car_id = row[0]
                old_price = row[1] if row[1] else 0
                new_price = price_int
                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand = %s,
                        model = %s,
                        variant = %s,
                        informasi_iklan = %s,
                        lokasi = %s,
                        price = %s,
                        year = %s,
                        millage = %s,
                        transmission = %s,
                        seat_capacity = %s,
                        gambar = %s,
                        last_scraped_at = %s
                    WHERE id = %s
                """
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
                    car_data.get("gambar"),  # kirim list Python
                    datetime.now(),
                    car_id
                ))
                if new_price != old_price and old_price != 0:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_id, old_price, new_price))
            else:
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                        (listing_url, brand, model, variant, informasi_iklan, lokasi, price,
                         year, millage, transmission, seat_capacity, gambar)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s, %s)
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
                    car_data.get("gambar")
                ))
            self.conn.commit()
            logging.info(f"✅ Data untuk listing_url={car_data['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def sync_to_cars(self):
        """
        Sinkronisasi data dari tabel {DB_TABLE_SCRAP} ke tabel {DB_TABLE_PRIMARY}.
        Jika listing_url sudah ada, lakukan update; jika tidak, insert.
        """
        logging.info(f"Memulai sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}...")
        try:
            fetch_query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(fetch_query)
            rows = self.cursor.fetchall()
            col_names = [desc[0] for desc in self.cursor.description]
            idx_url = col_names.index("listing_url")
            # indeks kolom lain bisa ditentukan sesuai kebutuhan
            for row in rows:
                listing_url = row[idx_url]
                check_query = f"SELECT id FROM {DB_TABLE_PRIMARY} WHERE listing_url = %s"
                self.cursor.execute(check_query, (listing_url,))
                result = self.cursor.fetchone()
                if result:
                    update_query = f"""
                        UPDATE {DB_TABLE_PRIMARY}
                        SET brand = %s,
                            model = %s,
                            variant = %s,
                            informasi_iklan = %s,
                            lokasi = %s,
                            price = %s,
                            year = %s,
                            millage = %s,
                            transmission = %s,
                            seat_capacity = %s,
                            gambar = %s,
                            last_scraped_at = %s
                        WHERE listing_url = %s
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
            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error saat sinkronisasi data: {e}")

    def export_data(self):
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
