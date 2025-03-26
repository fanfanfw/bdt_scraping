import os
import time
import logging
import re
import pandas as pd
import random
import psutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from .database import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history")

INPUT_FILE = os.getenv("INPUT_FILE", "mudahmy_brands.csv")

def count_chrome_processes():
    return len([p for p in psutil.process_iter(['name']) if p.info['name'] and 'chrome' in p.info['name'].lower()])
class MudahMyService:
    def __init__(self):
        self.driver = None
        self.stop_flag = False
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        self.batch_size = 25
        self.listing_count = 0

    def init_driver(self):
        logging.info("Menginisialisasi ChromeDriver...")
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        logging.info("ChromeDriver berhasil diinisialisasi.")

    def quit_driver(self):
        if self.driver:
            logging.info(f"Menutup ChromeDriver... (Chrome aktif: {count_chrome_processes()} sebelum quit)")
            try:
                self.driver.quit()
                logging.info(f"ChromeDriver berhasil ditutup. (Chrome aktif: {count_chrome_processes()} setelah quit)")
            except Exception as e:
                logging.error(f"Gagal menutup ChromeDriver: {e}")
            self.driver = None

    def get_listing_urls(self, listing_page_url):
        logging.info(f"📄 Mengambil listing dari: {listing_page_url}")
        if not self.driver:
            self.init_driver()

        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            try:
                self.driver.get(listing_page_url)
                time.sleep(random.uniform(1.5, 3.0))
                time.sleep(3)
                WebDriverWait(self.driver, 60).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@data-testid[contains(., 'listing-ad-item')]]//a"))
                )
                elements = self.driver.find_elements(By.XPATH, "//div[@data-testid[contains(., 'listing-ad-item')]]//a")
                urls = list(set(elem.get_attribute("href") for elem in elements if elem.get_attribute("href")))
                logging.info(f"✅ Ditemukan {len(urls)} listing URLs.")
                return urls
            except Exception as e:
                if "HTTPConnectionPool" in str(e):
                    attempt += 1
                    logging.error(f"❌ Error HTTPConnectionPool saat mengambil listing URLs: {e}. Mencoba lagi ({attempt}/{max_retries})...")
                    self.quit_driver()
                    time.sleep(5)
                    self.init_driver()
                else:
                    logging.error(f"❌ Error lain saat mengambil listing URLs: {e}. Tidak dilakukan retry.")
                    return []
        return []

    def convert_price_to_integer(self, price_string):
        price_clean = re.sub(r"[^\d]", "", price_string) if price_string else ""
        try:
            return int(price_clean)
        except ValueError:
            return None

    def scrape_detail(self, detail_url):
        if self.stop_flag:
            logging.info("⚠️ Scraping dihentikan sebelum mengambil detail.")
            return None

        if not self.driver:
            self.init_driver()

        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            logging.info(f"🔍 Mengambil detail dari: {detail_url}")
            try:
                self.driver.get(detail_url)
                time.sleep(random.uniform(1.5, 3.0))
                time.sleep(3)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                def extract(selector):
                    element = soup.select_one(selector)
                    return element.text.strip() if element else None

                detail = {
                    "listing_url": detail_url,
                    "price": self.convert_price_to_integer(extract("#ad_view_ad_highlights > div > div > div.flex.gap-1.md\\:items-end > div")),  # Konversi harga
                    "informasi_iklan": extract("#ad_view_ad_highlights > div > div > div:nth-child(1) > div > div > div"),
                    "year": extract("#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(1) > div"),
                    "transmission": extract("#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(2) > div"),
                    "millage": extract("#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(3) > div"),
                    "lokasi": extract("#ad_view_ad_highlights > div > div > div.flex.flex-wrap.lg\\:flex-nowrap.gap-3\\.5 > div:nth-child(4) > div"),
                    "brand": extract("#ad_view_car_specifications > div > div > div > div > div > div:nth-child(1) > div:nth-child(1) > div:nth-child(3)"),
                    "model": extract("#ad_view_car_specifications > div > div > div > div > div > div:nth-child(1) > div:nth-child(2) > div:nth-child(3)"),
                    "variant": extract("#ad_view_car_specifications > div > div > div > div > div > div:nth-child(1) > div:nth-child(4) > div:nth-child(3)"),
                    "seat_capacity": extract("#ad_view_car_specifications > div > div > div > div > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3)"),
                }

                img_elements = soup.select("img.w-full.h-full.bg-black.md\\:object-cover.object-contain")
                detail["gambar"] = [img["src"] for img in img_elements if "src" in img.attrs and "mudah" not in img["src"]]

                return detail
            except Exception as e:
                if "HTTPConnectionPool" in str(e):
                    attempt += 1
                    logging.error(f"❌ Error HTTPConnectionPool saat memuat detail {detail_url}: {e}. Mencoba lagi ({attempt}/{max_retries})...")
                    self.quit_driver()
                    time.sleep(5)
                    self.init_driver()
                else:
                    logging.error(f"❌ Error lain saat memuat detail {detail_url}: {e}. Tidak dilakukan retry.")
                    return None
        return None


    def scrape_all_brands(self, start_brand=None, start_model=None, start_page=1):
        try:
            self.reset_scraping()
            df = pd.read_csv(INPUT_FILE)

            # Jika brand & model disediakan, cari baris pertama yg cocok
            if start_brand and start_model:
                mask = (df['brand'] == start_brand) & (df['model'] == start_model)
                if mask.any():
                    start_index = df[mask].index[0]
                    df = df.iloc[start_index:]
                    logging.info(f"📌 Mulai scraping dari indeks {start_index} (brand: {start_brand}, model: {start_model})")
                else:
                    logging.warning("⚠️ Brand dan model tidak ditemukan di CSV, scraping seluruh data.")
            
            for _, row in df.iterrows():
                brand = row["brand"]
                model = row["model"]
                base_brand_url = row["url"]

                page_number = start_page if (brand == start_brand and model == start_model) else 1
                logging.info(f"🚀 Mulai scraping brand: {brand}, model: {model} dari halaman {page_number}")

                while not self.stop_flag:
                    current_url = f"{base_brand_url}?o={page_number}"
                    logging.info(f"📄 Scraping halaman {page_number}: {current_url}")

                    listing_urls = self.get_listing_urls(current_url)
                    if not listing_urls:
                        logging.info(f"✅ Tidak ditemukan listing URLs pada halaman {page_number}. Pindah ke data berikutnya.")
                        break

                    for listing_url in listing_urls:
                        if self.stop_flag:
                            break

                        detail = self.scrape_detail(listing_url)
                        if detail:
                            # Overwrite brand/model jika perlu
                            detail["brand"] = brand
                            detail["model"] = model
                            self.save_to_db_scrap(detail)
                            self.listing_count += 1

                            if self.listing_count >= self.batch_size:
                                logging.info(f"♻️ Batch {self.batch_size} listing tercapai, reinit driver...")
                                self.quit_driver()
                                time.sleep(5)
                                self.init_driver()
                                self.listing_count = 0

                    page_number += 1

            logging.info("✅ Proses scraping selesai.")
        except Exception as e:
            logging.error(f"❌ Error saat scraping: {e}")
        finally:
            self.quit_driver()

    def stop_scraping(self):
        logging.info("⚠️ Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

    def save_to_db_scrap(self, car_data):
        """Menyimpan data hasil scraping ke tabel DB_TABLE_SCRAP."""
        try:
            # Convert brand/model/variant ke uppercase agar konsisten
            if car_data['brand']:
                car_data['brand'] = car_data['brand'].upper()
            if car_data['model']:
                car_data['model'] = car_data['model'].upper()
            if car_data['variant']:
                car_data['variant'] = car_data['variant'].upper()

            select_query = f"SELECT id, price, previous_price FROM {DB_TABLE_SCRAP} WHERE listing_url = %s"
            self.cursor.execute(select_query, (car_data['listing_url'],))
            result = self.cursor.fetchone()

            if result:
                # UPDATE
                car_id, old_price, previous_price = result
                if car_data['price'] != old_price:
                    # Simpan perubahan harga ke tabel price_history
                    insert_history_query = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price, changed_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    """
                    self.cursor.execute(insert_history_query, (car_id, old_price, car_data['price']))

                    update_query = f"""
                        UPDATE {DB_TABLE_SCRAP}
                        SET brand = %s,
                            model = %s,
                            variant = %s,
                            informasi_iklan = %s,
                            lokasi = %s,
                            price = %s,
                            previous_price = %s,  -- Memperbarui previous_price
                            year = %s,
                            millage = %s,
                            transmission = %s,
                            seat_capacity = %s,
                            gambar = %s,
                            last_scraped_at = CURRENT_TIMESTAMP,
                            version = version + 1
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        car_data['brand'],
                        car_data['model'],
                        car_data['variant'],
                        car_data['informasi_iklan'],
                        car_data['lokasi'],
                        car_data['price'],
                        old_price,  
                        car_data['year'],
                        car_data['millage'],
                        car_data['transmission'],
                        car_data['seat_capacity'],
                        car_data['gambar'],
                        car_data['listing_url']
                    ))
            else:
                # INSERT
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                        (listing_url, brand, model, variant, informasi_iklan, lokasi, price,
                        year, millage, transmission, seat_capacity, gambar)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, (
                    car_data['listing_url'],
                    car_data['brand'],
                    car_data['model'],
                    car_data['variant'],
                    car_data['informasi_iklan'],
                    car_data['lokasi'],
                    car_data['price'],
                    car_data['year'],
                    car_data['millage'],
                    car_data['transmission'],
                    car_data['seat_capacity'],
                    car_data['gambar']
                ))

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan data scraping ke {DB_TABLE_SCRAP}: {e}")

    def sync_to_cars(self):
        """Sinkronisasi data dari DB_TABLE_SCRAP ke DB_TABLE_PRIMARY (cars)."""
        logging.info(f"Memulai sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY}...")
        try:
            fetch_query = f"SELECT * FROM {DB_TABLE_SCRAP};"
            self.cursor.execute(fetch_query)
            rows = self.cursor.fetchall()

            col_names = [desc[0] for desc in self.cursor.description]
            idx_url = col_names.index("listing_url")
            idx_brand = col_names.index("brand")
            idx_model = col_names.index("model")
            idx_variant = col_names.index("variant")
            idx_info = col_names.index("informasi_iklan")
            idx_loc = col_names.index("lokasi")
            idx_price = col_names.index("price")
            idx_year = col_names.index("year")
            idx_millage = col_names.index("millage")
            idx_trans = col_names.index("transmission")
            idx_seat = col_names.index("seat_capacity")
            idx_gambar = col_names.index("gambar")
            idx_last_scraped_at = col_names.index("last_scraped_at")
            idx_version = col_names.index("version")
            idx_created_at = col_names.index("created_at")
            idx_previous_price = col_names.index("previous_price")

            for row in rows:
                listing_url = row[idx_url]

                # Cek apakah listing_url sudah ada di DB_TABLE_PRIMARY
                check_query = f"SELECT id FROM {DB_TABLE_PRIMARY} WHERE listing_url = %s"
                self.cursor.execute(check_query, (listing_url,))
                result = self.cursor.fetchone()

                if result:
                    # UPDATE jika listing_url ada
                    update_query = f"""
                        UPDATE {DB_TABLE_PRIMARY}
                        SET brand = %s,
                            model = %s,
                            variant = %s,
                            informasi_iklan = %s,
                            lokasi = %s,
                            price = %s,
                            previous_price = %s,  -- Memperbarui previous_price
                            year = %s,
                            millage = %s,
                            transmission = %s,
                            seat_capacity = %s,
                            gambar = %s,
                            last_scraped_at = %s,
                            version = %s,
                            created_at = %s
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        row[idx_brand],
                        row[idx_model],
                        row[idx_variant],
                        row[idx_info],
                        row[idx_loc],
                        row[idx_price],
                        row[idx_previous_price],  
                        row[idx_year],
                        row[idx_millage],
                        row[idx_trans],
                        row[idx_seat],
                        row[idx_gambar],
                        row[idx_last_scraped_at],
                        row[idx_version],
                        row[idx_created_at],
                        listing_url
                    ))

                else:
                    # INSERT jika listing_url tidak ada di tabel cars
                    insert_query = f"""
                        INSERT INTO {DB_TABLE_PRIMARY}
                            (listing_url, brand, model, variant, informasi_iklan, lokasi,
                            price, year, millage, transmission, seat_capacity, gambar,
                            last_scraped_at, version, created_at, previous_price)
                        VALUES
                            (%s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        listing_url,
                        row[idx_brand],
                        row[idx_model],
                        row[idx_variant],
                        row[idx_info],
                        row[idx_loc],
                        row[idx_price],
                        row[idx_year],
                        row[idx_millage],
                        row[idx_trans],
                        row[idx_seat],
                        row[idx_gambar],
                        row[idx_last_scraped_at],
                        row[idx_version],
                        row[idx_created_at],
                        row[idx_price]  
                    ))

            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error saat sinkronisasi data: {e}")