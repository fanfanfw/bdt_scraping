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
            return millage_value * 1000 if millage_value < 1000 else millage_value
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

        brand = extract("...li:nth-child(3) > a > span")
        model = extract("...li:nth-child(4) > a > span")
        variant = extract("...li:nth-child(5) > a > span")

        if not variant:
            variant = "NO VARIANT"

        informasi_iklan = extract("...masthead... > div:nth-child(1) > span")
        lokasi_part1 = extract("...span:nth-child(2)")
        lokasi_part2 = extract("...span:nth-child(3)")
        lokasi = " ".join(filter(None, [lokasi_part1, lokasi_part2]))

        gambar = []
        gambar_container = soup.select_one("#details-gallery > div > div")
        if gambar_container:
            for img in gambar_container.find_all("img"):
                src = img.get("src")
                if src:
                    gambar.append(src)

        price_string = extract("...listing__item-price > h3")
        price = self.convert_price_to_integer(price_string)
        year = extract("...div:nth-child(2)...u-text-bold")
        millage = extract("...div:nth-child(3)...u-text-bold")
        transmission = extract("...div:nth-child(6)...u-text-bold")
        seat_capacity = extract("...div:nth-child(7)...u-text-bold")

        return {
            "listing_url": url,
            "brand": brand.upper() if brand else None,
            "model": model.upper() if model else None,
            "variant": variant,
            "informasi_iklan": informasi_iklan,
            "lokasi": lokasi,
            "price": price,
            "year": year,
            "millage": convert_millage(millage),
            "transmission": transmission,
            "seat_capacity": seat_capacity,
            "gambar": gambar
        }

    def save_to_db(self, car_data):
        try:
            select_query = f"SELECT id, price FROM {DB_TABLE_SCRAP} WHERE listing_url = %s"
            self.cursor.execute(select_query, (car_data['listing_url'],))
            result = self.cursor.fetchone()

            if result:
                car_id, current_price = result

                if car_data['price'] != current_price:
                    insert_history = f"""
                        INSERT INTO {DB_TABLE_HISTORY_PRICE} (car_id, old_price, new_price)
                        VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(insert_history, (car_id, current_price, car_data['price']))

                    update_query = f"""
                        UPDATE {DB_TABLE_SCRAP}
                        SET price = %s,
                            informasi_iklan = %s,
                            lokasi = %s,
                            year = %s,
                            millage = %s,
                            transmission = %s,
                            seat_capacity = %s,
                            gambar = %s,
                            brand = %s,
                            model = %s,
                            variant = %s,
                            last_scraped_at = CURRENT_TIMESTAMP
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        car_data['price'], car_data['informasi_iklan'], car_data['lokasi'], car_data['year'],
                        car_data['millage'], car_data['transmission'], car_data['seat_capacity'],
                        car_data['gambar'], car_data['brand'], car_data['model'], car_data['variant'],
                        car_data['listing_url']
                    ))
            else:
                insert_query = f"""
                    INSERT INTO {DB_TABLE_SCRAP}
                    (listing_url, brand, model, variant, informasi_iklan, lokasi, price,
                     year, millage, transmission, seat_capacity, gambar, last_scraped_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                self.cursor.execute(insert_query, (
                    car_data['listing_url'], car_data['brand'], car_data['model'], car_data['variant'],
                    car_data['informasi_iklan'], car_data['lokasi'], car_data['price'],
                    car_data['year'], car_data['millage'], car_data['transmission'],
                    car_data['seat_capacity'], car_data['gambar']
                ))

            self.conn.commit()
            logging.info(f"✅ Data untuk {car_data['listing_url']} berhasil disimpan/diupdate.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error menyimpan atau memperbarui data ke database: {e}")

    def stop_scraping(self):
        self.stop_flag = True
        logging.info("🛑 Scraping dihentikan oleh user.")
