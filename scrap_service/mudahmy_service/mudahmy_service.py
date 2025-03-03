import os
import time
import logging
import re
import pandas as pd
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

INPUT_FILE = "/home/fanfan/scraping/scrap_service/mudahmy_service/storage/input_files/mudahMY_scraplist.csv"

class MudahMyService:
    def __init__(self):
        self.driver = None
        self.stop_flag = False
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

        # Pengaturan batch
        self.batch_size = 80   
        self.listing_count = 0  

    def init_driver(self):
        """Inisialisasi (atau re-inisialisasi) ChromeDriver."""
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
        """Menutup driver untuk membebaskan resource."""
        if self.driver:
            logging.info("Menutup ChromeDriver...")
            try:
                self.driver.quit()
                logging.info("ChromeDriver berhasil ditutup.")
            except Exception as e:
                logging.error(f"Gagal menutup ChromeDriver: {e}")
            self.driver = None

    def get_listing_urls(self, listing_page_url):
        """Mengambil semua URL listing dari halaman brand."""
        logging.info(f"📄 Mengambil listing dari: {listing_page_url}")
        if not self.driver:
            self.init_driver()

        try:
            self.driver.get(listing_page_url)
            time.sleep(3)
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[@data-testid[contains(., 'listing-ad-item')]]//a"))
            )
            elements = self.driver.find_elements(By.XPATH, "//div[@data-testid[contains(., 'listing-ad-item')]]//a")
            urls = list(set(elem.get_attribute("href") for elem in elements if elem.get_attribute("href")))
            logging.info(f"✅ Ditemukan {len(urls)} listing URLs.")
            return urls
        except Exception as e:
            logging.error(f"❌ Error mengambil listing URLs: {e}")
            return []

    def scrape_detail(self, detail_url):
        """Scraping detail dari satu listing."""
        if self.stop_flag:
            logging.info("⚠️ Scraping dihentikan sebelum mengambil detail.")
            return None

        if not self.driver:
            self.init_driver()

        logging.info(f"🔍 Mengambil detail dari: {detail_url}")
        try:
            self.driver.get(detail_url)
            time.sleep(3)
        except Exception as e:
            logging.error(f"Error saat memuat halaman detail {detail_url}: {e}")
            return None

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        def extract(selector):
            element = soup.select_one(selector)
            return element.text.strip() if element else None

        detail = {
            "listing_url": detail_url,
            "price": extract("#ad_view_ad_highlights > div > div > div.flex.gap-1.md\\:items-end > div"),
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

    def scrape_all_brands(self, start_brand=None, start_page=1):
        """Scrape semua brand berdasarkan file CSV dengan metode batch."""
        try:
            self.reset_scraping()
            df = pd.read_csv(INPUT_FILE)
            start_scraping = False if start_brand else True
            
            for _, row in df.iterrows():
                brand = row["brand"]
                base_brand_url = row["url"]
                
                if not start_scraping:
                    if brand == start_brand:
                        start_scraping = True
                    else:
                        continue

                logging.info(f"🚀 Mulai scraping brand: {brand}")
                page_number = start_page if brand == start_brand else 1

                while not self.stop_flag:
                    current_url = f"{base_brand_url}?o={page_number}"
                    logging.info(f"📄 Scraping halaman {page_number}: {current_url}")

                    listing_urls = self.get_listing_urls(current_url)
                    if not listing_urls:
                        logging.info(f"✅ Tidak ditemukan listing URLs pada halaman {page_number}. Menghentikan scraping brand: {brand}")
                        break

                    for listing_url in listing_urls:
                        if self.stop_flag:
                            break
                        detail = self.scrape_detail(listing_url)
                        if detail:
                            self.save_to_db(detail)
                            self.listing_count += 1

                            # Jika sudah mencapai batch_size, reinitialize driver
                            if self.listing_count >= self.batch_size:
                                logging.info(f"Batch {self.batch_size} listing tercapai, reinit driver...")
                                self.quit_driver()
                                time.sleep(5)  # jeda sejenak
                                self.init_driver()
                                self.listing_count = 0

                    page_number += 1
            
            logging.info("✅ Proses scraping semua brand selesai.")
        except Exception as e:
            logging.error(f"❌ Error saat scraping semua brand: {e}")
        finally:
            # Pastikan driver ditutup pada akhirnya
            self.quit_driver()

    def stop_scraping(self):
        logging.info("⚠️ Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

    def save_to_db(self, car_data):
        """Menyimpan atau memperbarui data mobil ke database PostgreSQL."""
        try:
            select_query = "SELECT id FROM cars WHERE listing_url = %s"
            self.cursor.execute(select_query, (car_data['listing_url'],))
            result = self.cursor.fetchone()

            if result:  # Data sudah ada, update
                update_query = """
                    UPDATE cars
                    SET price = %s, informasi_iklan = %s, lokasi = %s, year = %s, millage = %s,
                         transmission = %s, seat_capacity = %s, gambar = %s,
                         last_scraped_at = CURRENT_TIMESTAMP, version = version + 1
                    WHERE listing_url = %s
                """
                self.cursor.execute(update_query, (
                    car_data['price'], car_data['informasi_iklan'], car_data['lokasi'],
                    car_data['year'], car_data['millage'],
                    car_data['transmission'], car_data['seat_capacity'], car_data['gambar'],
                    car_data['listing_url']
                ))
            else:  # Data belum ada, insert
                insert_query = """
                    INSERT INTO cars (listing_url, brand, model, variant, informasi_iklan, lokasi, price,
                                      year, millage, transmission, seat_capacity, gambar)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, (
                    car_data['listing_url'], car_data['brand'], car_data['model'], car_data['variant'],
                    car_data['informasi_iklan'], car_data['lokasi'], car_data['price'], car_data['year'],
                    car_data['millage'], car_data['transmission'],
                    car_data['seat_capacity'], car_data['gambar']
                ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def close(self):
        """Menutup driver dan koneksi database."""
        self.quit_driver()
        self.cursor.close()
        self.conn.close()
        logging.info("Koneksi database ditutup, driver Selenium ditutup.")
