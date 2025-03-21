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
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

INPUT_FILE = "scrap_service/carlistmy_service/storage/input_files/carlistMY_scraplist.csv"

class CarlistMyService:
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
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.driver.set_page_load_timeout(120)
        logging.info("ChromeDriver berhasil diinisialisasi.")


    def convert_price_to_integer(self, price_string):
        """Mengonversi harga dalam format string (misalnya "RM 38,800") ke tipe data INTEGER"""
        if not price_string:  # Cek jika price_string adalah None atau string kosong
            return None
        
        price_clean = re.sub(r"[^\d]", "", price_string)
        
        try:
            return int(price_clean)  # Konversi menjadi integer
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
                time.sleep(5)
                break
            except Exception as e:
                if "HTTPConnectionPool" in str(e):
                    attempt += 1
                    logging.error(f"❌ Error HTTPConnectionPool saat memuat halaman detail {detail_url}: {e}. Mencoba lagi ({attempt}/{max_retries})...")
                    self.quit_driver()
                    time.sleep(5)
                    self.init_driver()
                else:
                    logging.error(f"❌ Error lain saat memuat halaman detail {detail_url}: {e}. Tidak dilakukan retry.")
                    self.debug_dump("scrape_detail_error")
                    return None
        else:
            logging.error(f"❌ Gagal memuat halaman detail {detail_url} setelah {max_retries} percobaan.")
            return None

        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        def extract(selector):
            element = soup.select_one(selector)
            return element.text.strip() if element else None

        brand = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(3) > a > span")
        model = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(4) > a > span")
        variant = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(5) > a > span")

        informasi_iklan = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--masthead.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-2\\@mobile > div > div > div:nth-child(1) > span.u-color-muted.u-text-7.u-hide\\@mobile")
        
        lokasi_part1 = extract("#listing-detail > section:nth-child(2) > div > div > div.c-sidebar.c-sidebar--top.u-width-2\\/6.u-width-1\\@mobile.u-padding-right-sm.u-padding-left-md.u-padding-top-md.u-padding-top-none\\@mobile.u-flex.u-flex--column.u-flex--column\\@mobile.u-order-first\\@mobile > div.c-card.c-card--ctr.u-margin-ends-sm.u-order-last\\@mobile > div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(2)")
        lokasi_part2 = extract("#listing-detail > section:nth-child(2) > div > div > div.c-sidebar.c-sidebar--top.u-width-2\\/6.u-width-1\\@mobile.u-padding-right-sm.u-padding-left-md.u-padding-top-md.u-padding-top-none\\@mobile.u-flex.u-flex--column.u-flex--column\\@mobile.u-order-first\\@mobile > div.c-card.c-card--ctr.u-margin-ends-sm.u-order-last\\@mobile > div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(3)")
        lokasi = " ".join(filter(None, [lokasi_part1, lokasi_part2]))

        gambar_container = soup.select_one("#details-gallery > div > div")
        gambar = []
        if gambar_container:
            img_tags = gambar_container.find_all("img")
            for img in img_tags:
                src = img.get("src")
                if src:
                    gambar.append(src)

        price_string = extract("#details-gallery > div > div > div.c-gallery--hero-img.u-relative > div.c-gallery__item > div.c-gallery__item-details.u-padding-lg.u-padding-md\\@mobile.u-absolute.u-bottom-right.u-bottom-left.u-zindex-1 > div > div.listing__item-price > h3")
        price = self.convert_price_to_integer(price_string)

        year = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(2) > div > div > div > span.u-text-bold.u-block")
        millage = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(3) > div > div > div > span.u-text-bold.u-block")
        transmission = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(6) > div > div > div > span.u-text-bold.u-block")
        seat_capacity = extract("#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(7) > div > div > div > span.u-text-bold.u-block")

        detail = {
            "listing_url": detail_url,
            "brand": brand,
            "model": model,
            "variant": variant,
            "informasi_iklan": informasi_iklan,
            "lokasi": lokasi,
            "price": price,  # Gunakan harga yang sudah dikonversi
            "year": year,
            "millage": millage,
            "transmission": transmission,
            "seat_capacity": seat_capacity,
            "gambar": gambar
        }
        return detail

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
                        
    def debug_dump(self, prefix):
        """Simpan screenshot dan page_source untuk keperluan debugging."""
        timestamp = int(time.time())
        screenshot_name = f"{prefix}_{timestamp}.png"
        html_name = f"{prefix}_{timestamp}.html"

        try:
            self.driver.save_screenshot(screenshot_name)
            logging.info(f"Screenshot disimpan: {screenshot_name}")
        except Exception as e:
            logging.error(f"Gagal mengambil screenshot: {e}")

        try:
            page_source = self.driver.page_source
            with open(html_name, "w", encoding="utf-8") as f:
                f.write(page_source)
            logging.info(f"HTML page source disimpan: {html_name}")
        except Exception as e:
            logging.error(f"Gagal menyimpan HTML page source: {e}")

    def get_listing_urls(self, listing_page_url):
        logging.info(f"📄 Mengambil listing dari: {listing_page_url}")

        if not self.driver:
            self.init_driver()

        max_retries = 3
        attempt = 0
        while attempt < max_retries:
            try:
                self.driver.get(listing_page_url)
                time.sleep(3)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.ellipsize.js-ellipsize-text"))
                )
                elements = self.driver.find_elements(By.CSS_SELECTOR, "a.ellipsize.js-ellipsize-text")
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
                    self.debug_dump("get_listing_urls_error")
                    return []
        return []

    def scrape_all_brands(self, start_brand=None, start_page=1):
        """Scrape semua brand berdasarkan file CSV dengan opsi batch untuk meminimalkan penggunaan memori."""
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
                    try:
                        # Format URL dengan page_number
                        paginated_url = re.sub(r"(page_number=)\d+", lambda m: m.group(1) + str(page_number), base_brand_url)
                        logging.info(f"📄 Scraping halaman {page_number}: {paginated_url}")
                        
                        listing_urls = self.get_listing_urls(paginated_url)
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

                                # Jika sudah mencapai batch_size, restart driver untuk menghemat memori
                                if self.listing_count >= self.batch_size:
                                    logging.info(f"Batch {self.batch_size} listing tercapai, reinit driver...")
                                    self.quit_driver()
                                    time.sleep(3)  
                                    self.init_driver()
                                    self.listing_count = 0

                        page_number += 1

                    except Exception as e:
                        logging.error(f"❌ Error saat scraping halaman {page_number}: {e}")
                        
                        # Jika error karena timeout, restart driver dan lanjut dari halaman terakhir
                        if "timeout" in str(e).lower():
                            logging.warning(f"⚠️ Timeout terjadi. Restarting ChromeDriver...")
                            self.quit_driver()
                            time.sleep(5)
                            self.init_driver()
                            logging.info(f"Melanjutkan scraping dari halaman {page_number} untuk brand {brand}...")

                        else:
                            logging.error("❌ Error fatal, menghentikan scraping brand ini.")
                            break  # Lewati brand jika error terlalu fatal
                
            logging.info("✅ Proses scraping semua brand selesai.")
        except Exception as e:
            logging.error(f"❌ Error saat scraping semua brand: {e}")
        finally:
            self.quit_driver()


    def stop_scraping(self):
        logging.info("⚠️ Permintaan untuk menghentikan scraping diterima.")
        self.stop_flag = True

    def reset_scraping(self):
        self.stop_flag = False
        self.listing_count = 0
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

    def save_to_db(self, car_data):
        """Menyimpan atau memperbarui data mobil ke database PostgreSQL dengan mencatat perubahan harga."""
        try:
            select_query = "SELECT id, price, previous_price FROM cars WHERE listing_url = %s"
            self.cursor.execute(select_query, (car_data['listing_url'],))
            result = self.cursor.fetchone()

            if result:  # Data sudah ada, update
                car_id, current_price, previous_price = result

                # Cek jika harga berubah
                if car_data['price'] != current_price:
                    # Simpan perubahan harga ke tabel price_history
                    self.cursor.execute("""
                        INSERT INTO price_history (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """, (car_id, current_price, car_data['price']))

                    # Update kolom previous_price
                    update_query = """
                        UPDATE cars
                        SET price = %s, previous_price = %s, informasi_iklan = %s, lokasi = %s, year = %s, 
                            millage = %s, transmission = %s, seat_capacity = %s, gambar = %s,
                            last_scraped_at = CURRENT_TIMESTAMP, version = version + 1
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        car_data['price'], current_price, car_data['informasi_iklan'], car_data['lokasi'],
                        car_data['year'], car_data['millage'], car_data['transmission'], car_data['seat_capacity'],
                        car_data['gambar'], car_data['listing_url']
                    ))
                else:
                    # Jika harga tidak berubah, update informasi lainnya
                    update_query = """
                        UPDATE cars
                        SET informasi_iklan = %s, lokasi = %s, year = %s, millage = %s,
                            transmission = %s, seat_capacity = %s, gambar = %s,
                            last_scraped_at = CURRENT_TIMESTAMP, version = version + 1
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        car_data['informasi_iklan'], car_data['lokasi'], car_data['year'], car_data['millage'],
                        car_data['transmission'], car_data['seat_capacity'], car_data['gambar'], car_data['listing_url']
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
                    car_data['millage'], car_data['transmission'], car_data['seat_capacity'], car_data['gambar']
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
