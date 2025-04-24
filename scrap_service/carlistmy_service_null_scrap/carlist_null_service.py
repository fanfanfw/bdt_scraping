import os
import time
import logging
import random
import re
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from scrap_service.carlistmy_service_null_scrap.database import get_connection

DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history")

PROXY_MODE = os.getenv("PROXY_MODE", "none")
CUSTOM_PROXIES = os.getenv("CUSTOM_PROXIES", "")

log_folder = "logs"
os.makedirs(log_folder, exist_ok=True)
log_start_date = datetime.now().strftime("%Y%m%d")
log_file_path = os.path.join(log_folder, f"scrape_carlistmy_null_{log_start_date}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def take_screenshot(driver, name: str):
    try:
        error_folder_name = time.strftime('%Y%m%d') + "_error_carlistmy_null"
        screenshot_dir = os.path.join("logs", error_folder_name)
        if not os.path.exists(screenshot_dir):
            os.makedirs(screenshot_dir)

        timestamp = time.strftime('%H%M%S')
        screenshot_path = os.path.join(screenshot_dir, f"{name}_{timestamp}.png")
        driver.save_screenshot(screenshot_path)
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")

def convert_millage(millage_str):
    if isinstance(millage_str, int):
        return millage_str
    if isinstance(millage_str, str):
        numbers = re.findall(r'\d+', millage_str)
        if numbers:
            millage_value = int(numbers[-1])
            if millage_value >= 1000:
                return millage_value
            else:
                return millage_value * 1000
    return None

class CarlistMyNullService:
    def __init__(self):
        self.driver = None
        self.stop_flag = False
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.load_proxies()

    def load_proxies(self):
        self.proxies = [proxy.split(":") for proxy in CUSTOM_PROXIES.split(",") if proxy]

    def init_driver(self, proxy=None):
        logging.info("Menginisialisasi ChromeDriver dengan Selenium Wire...")
        options = Options()
        seleniumwire_options = {}

        if proxy:
            proxy_address, proxy_port, proxy_user, proxy_pass = proxy
            seleniumwire_options = {
                'proxy': {
                    'http': f'http://{proxy_user}:{proxy_pass}@{proxy_address}:{proxy_port}',
                    'https': f'http://{proxy_user}:{proxy_pass}@{proxy_address}:{proxy_port}',
                    'no_proxy': 'localhost,127.0.0.1'
                },
                'disable_capture': True
            }
            logging.info(f"🌐 Menggunakan proxy: {proxy_address}:{proxy_port}")

        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=seleniumwire_options)
        self.driver.set_page_load_timeout(120)
        self.check_ip()

    def check_ip(self):
        try:
            self.driver.get("https://ip.oxylabs.io/")
            time.sleep(2)
            ip = self.driver.find_element(By.TAG_NAME, 'body').text.strip()
            logging.info(f"🌐 IP yang digunakan oleh proxy: {ip}")
            return ip
        except Exception as e:
            logging.warning(f"❌ Gagal memverifikasi IP: {e}")
            take_screenshot(self.driver, "ip_check_error")
            return None

    def quit_driver(self):
        if self.driver:
            logging.info("Menutup ChromeDriver...")
            try:
                self.driver.quit()
            except Exception as e:
                logging.error(f"Gagal menutup ChromeDriver: {e}")
            self.driver = None

    def scrape_null_entries(self):
        logging.info("🔍 Memulai scraping ulang data NULL...")

        count_query = f"""
            SELECT COUNT(*) FROM {DB_TABLE_SCRAP}
            WHERE brand IS NULL OR model IS NULL OR variant IS NULL OR price IS NULL
        """
        self.cursor.execute(count_query)
        total_count = self.cursor.fetchone()[0]
        logging.info(f"📊 Total listing dengan data NULL yang akan diproses: {total_count}")

        query = f"""
            SELECT listing_url FROM {DB_TABLE_SCRAP}
            WHERE brand IS NULL OR model IS NULL OR variant IS NULL OR price IS NULL
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        urls = [row[0] for row in rows if row[0]]

        processed = 0
        batch_size = 25

        for i, url in enumerate(urls):
            if self.stop_flag:
                break

            if processed % batch_size == 0:
                self.quit_driver()
                time.sleep(random.uniform(2, 5))
                if self.proxies:
                    proxy = random.choice(self.proxies)
                    self.init_driver(proxy)
                else:
                    self.init_driver()

            try:
                self.driver.get(url)
                time.sleep(5)
                detail = self.scrape_detail(url)
                if detail:
                    self.save_to_db(detail)
            except Exception as e:
                logging.error(f"Gagal scraping URL {url}: {e}")
                take_screenshot(self.driver, "scrape_detail_error")

            processed += 1
            logging.info(f"✅ Progress: {processed}/{total_count}")
            time.sleep(random.randint(15, 20))

        self.quit_driver()

    def convert_price_to_integer(self, price_str):
        """
        Mengubah harga dalam bentuk string seperti 'RM 83,000' atau 'RM83K' ke integer 83000.
        """
        try:
            price_str = price_str.replace("RM", "").replace(",", "").strip().upper()
            if "K" in price_str:
                return int(float(price_str.replace("K", "")) * 1000)
            return int(float(price_str))
        except:
            return None

    def scrape_detail(self, url):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        def extract(selector):
            element = soup.select_one(selector)
            return element.text.strip() if element else None

        brand = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(3) > a > span")
        model = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(4) > a > span")
        variant = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--breadcrumb.u-margin-top-xs.u-hide\\@mobile.js-part-breadcrumb > div > ul > li:nth-child(5) > a > span")

        if not variant:
            variant = "NO VARIANT"

        informasi_iklan = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--masthead.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-2\\@mobile > div > div > div:nth-child(1) > span.u-color-muted.u-text-7.u-hide\\@mobile")

        lokasi_part1 = extract(
            "#listing-detail > section:nth-child(2) > div > div > div.c-sidebar.c-sidebar--top.u-width-2\\/6.u-width-1\\@mobile.u-padding-right-sm.u-padding-left-md.u-padding-top-md.u-padding-top-none\\@mobile.u-flex.u-flex--column.u-flex--column\\@mobile.u-order-first\\@mobile > div.c-card.c-card--ctr.u-margin-ends-sm.u-order-last\\@mobile > div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(2)")
        lokasi_part2 = extract(
            "#listing-detail > section:nth-child(2) > div > div > div.c-sidebar.c-sidebar--top.u-width-2\\/6.u-width-1\\@mobile.u-padding-right-sm.u-padding-left-md.u-padding-top-md.u-padding-top-none\\@mobile.u-flex.u-flex--column.u-flex--column\\@mobile.u-order-first\\@mobile > div.c-card.c-card--ctr.u-margin-ends-sm.u-order-last\\@mobile > div.c-card__body > div.u-flex.u-align-items-center > div > div > span:nth-child(3)")
        lokasi = " ".join(filter(None, [lokasi_part1, lokasi_part2]))

        gambar_container = soup.select_one("#details-gallery > div > div")
        gambar = []
        if gambar_container:
            img_tags = gambar_container.find_all("img")
            for img in img_tags:
                src = img.get("src")
                if src:
                    gambar.append(src)

        price_string = extract(
            "#details-gallery > div > div > div.c-gallery--hero-img.u-relative > div.c-gallery__item > div.c-gallery__item-details.u-padding-lg.u-padding-md\\@mobile.u-absolute.u-bottom-right.u-bottom-left.u-zindex-1 > div > div.listing__item-price > h3")
        price = self.convert_price_to_integer(price_string)

        year = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(2) > div > div > div > span.u-text-bold.u-block")
        millage = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(3) > div > div > div > span.u-text-bold.u-block")
        transmission = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(6) > div > div > div > span.u-text-bold.u-block")
        seat_capacity = extract(
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs.u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--key-details.u-margin-ends-lg.u-margin-ends-sm\\@mobile.u-order-3\\@mobile > div > div > div > div > div.owl-stage-outer > div > div:nth-child(7) > div > div > div > span.u-text-bold.u-block")

        status = "sold" if "this car has already been sold" in soup.get_text().lower() else "active"
        sold_at = datetime.now() if status == "sold" else None

        detail = {
            "listing_url": url,
            "brand": brand.upper() if brand else None,
            "model": model.upper() if model else None,
            "variant": variant.upper() if variant else None,
            "informasi_iklan": informasi_iklan,
            "lokasi": lokasi,
            "price": price,
            "year": year,
            "millage": convert_millage(millage),
            "transmission": transmission,
            "seat_capacity": seat_capacity,
            "gambar": gambar,
            "status": status,
            "sold_at": sold_at  # <--- ini penting!
        }
        return detail

    def save_to_db(self, car_data):
        try:
            select_query = f"SELECT id, price FROM {DB_TABLE_SCRAP} WHERE listing_url = %s"
            self.cursor.execute(select_query, (car_data['listing_url'],))
            result = self.cursor.fetchone()

            if result:
                car_id, current_price = result

                # Selalu update data + waktu scraping
                update_query = f"""
                    UPDATE {DB_TABLE_SCRAP}
                    SET brand = %s,
                        model = %s,
                        variant = %s,
                        price = %s,
                        informasi_iklan = %s,
                        lokasi = %s,
                        year = %s,
                        millage = %s,
                        transmission = %s,
                        seat_capacity = %s,
                        gambar = %s,
                        status = %s,
                        sold_at = %s,
                        last_scraped_at = CURRENT_TIMESTAMP
                    WHERE listing_url = %s
                """
                self.cursor.execute(update_query, (
                    car_data['brand'], car_data['model'], car_data['variant'],
                    car_data['price'], car_data['informasi_iklan'], car_data['lokasi'],
                    car_data['year'], car_data['millage'], car_data['transmission'],
                    car_data['seat_capacity'], car_data['gambar'],
                    car_data['status'], car_data['sold_at'],
                    car_data['listing_url']
                ))

                if car_data['price'] != current_price:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_id, current_price, car_data['price']))

            else:
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                    (listing_url, brand, model, variant, informasi_iklan, lokasi, price,
                     year, millage, transmission, seat_capacity, gambar, status, sold_at, last_scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                self.cursor.execute(insert_query, (
                    car_data['listing_url'], car_data['brand'], car_data['model'], car_data['variant'],
                    car_data['informasi_iklan'], car_data['lokasi'], car_data['price'],
                    car_data['year'], car_data['millage'], car_data['transmission'],
                    car_data['seat_capacity'], car_data['gambar'],
                    car_data['status'], car_data['sold_at']
                ))

            self.conn.commit()
            logging.info(f"✅ Data untuk {car_data['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def stop_scraping(self):
        self.stop_flag = True
        logging.info("🛑 Scraping dihentikan oleh user.")
