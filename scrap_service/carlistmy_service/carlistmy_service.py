import os
import time
from datetime import datetime
import logging
import re
import random
import pandas as pd
import json
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from scrap_service.carlistmy_service.database import get_connection

log_folder = "logs"
os.makedirs(log_folder, exist_ok=True)

# Buat nama file log berdasarkan tanggal saat program pertama kali dijalankan
log_start_date = datetime.now().strftime("%Y%m%d")
log_file_path = os.path.join(log_folder, f"scrape_carlistmy_{log_start_date}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler()  # agar tetap tampil di terminal
    ]
)

DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")
DB_TABLE_HISTORY_PRICE = os.getenv("DB_TABLE_HISTORY_PRICE", "price_history")

INPUT_FILE = os.getenv("INPUT_FILE", "carlistmy_brands.csv")

PROXY_MODE = os.getenv("PROXY_MODE", "none")
CUSTOM_PROXIES = os.getenv("CUSTOM_PROXIES", "")

# Mengonversi daftar proxy dari string ke list of tuples
if PROXY_MODE == "custom" and CUSTOM_PROXIES:
    proxies = [
        proxy.split(":") for proxy in CUSTOM_PROXIES.split(",")
    ]
else:
    proxies = None

def take_screenshot(driver, name: str):
    """
    Simpan screenshot ke dalam folder logs/<YYYYMMDD>_error/
    """
    try:
        error_folder_name = time.strftime('%Y%m%d') + "_error_carlistmy"
        screenshot_dir = os.path.join("logs", error_folder_name)
        if not os.path.exists(screenshot_dir):
            os.makedirs(screenshot_dir)

        timestamp = time.strftime('%H%M%S')
        screenshot_path = os.path.join(screenshot_dir, f"{name}_{timestamp}.png")

        driver.save_screenshot(screenshot_path)
        logging.info(f"📸 Screenshot disimpan: {screenshot_path}")
    except Exception as e:
        logging.warning(f"❌ Gagal menyimpan screenshot: {e}")


class CarlistMyService:
    def __init__(self):
        self.driver = None
        self.stop_flag = False
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.listing_count = 0

    def init_driver(self, proxy=None):
        """Menyiapkan WebDriver dengan proxy yang diterapkan termasuk autentikasi"""
        logging.info("Menginisialisasi ChromeDriver dengan Selenium Wire...")
        options = Options()

        # Jika menggunakan proxy, atur konfigurasi proxy
        if proxy:
            proxy_address, proxy_port, proxy_user, proxy_password = proxy
            logging.info(f"🌐 Menggunakan proxy dengan autentikasi: {proxy_address}:{proxy_port}")
            seleniumwire_options = {
                'proxy': {
                    'http': f'http://{proxy_user}:{proxy_password}@{proxy_address}:{proxy_port}',
                    'https': f'http://{proxy_user}:{proxy_password}@{proxy_address}:{proxy_port}',
                    'no_proxy': 'localhost,127.0.0.1',
                },
                'disable_capture': True
            }
        else:
            seleniumwire_options = {}

        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

        # Menggunakan Selenium Wire untuk inisialisasi WebDriver
        service = Service(ChromeDriverManager().install())

        # Inisialisasi WebDriver dengan seleniumwire_options
        self.driver = webdriver.Chrome(service=service, options=options, seleniumwire_options=seleniumwire_options)

        self.driver.set_page_load_timeout(120)
        logging.info("ChromeDriver berhasil diinisialisasi dengan Selenium Wire.")

        # Verifikasi apakah proxy terpasang dengan benar
        self.check_ip()

    def check_ip(self):
        """Memverifikasi IP yang digunakan oleh proxy dengan mengunjungi https://ip.oxylabs.io/"""
        try:
            self.driver.get("https://ip.oxylabs.io/")
            time.sleep(2)  # Tunggu sampai halaman dimuat
            ip = self.driver.find_element(By.TAG_NAME, 'body').text.strip()
            logging.info(f"🌐 IP yang digunakan oleh proxy: {ip}")
            return ip
        except Exception as e:
            logging.warning(f"❌ Gagal memverifikasi IP: {e}")
            return None

    def get_current_ip(self):
        """Memverifikasi IP yang digunakan oleh proxy dengan mengunjungi https://ip.oxylabs.io/"""
        try:
            self.driver.get("https://ip.oxylabs.io/")
            time.sleep(2)  # Tunggu sampai halaman dimuat
            ip = self.driver.find_element(By.TAG_NAME, 'body').text.strip()
            logging.info(f"🌐 IP yang digunakan oleh proxy: {ip}")
            return ip
        except Exception as e:
            logging.warning(f"❌ Gagal memverifikasi IP: {e}")
            take_screenshot(self.driver, "ip_check_error")
            return None

    def convert_price_to_integer(self, price_string):
        """Mengonversi harga dalam format string (misalnya 'RM 38,800') ke tipe data INTEGER"""
        if not price_string:
            return None
        price_clean = re.sub(r"[^\d]", "", price_string)
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
                time.sleep(5)
                break
            except Exception as e:
                logging.error(f"❌ Error saat memuat halaman detail {detail_url}: {e}")
                take_screenshot(self.driver, "scrape_detail_error")
                self.quit_driver()
                time.sleep(5)
                self.init_driver()
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
            "price": price,
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
                    logging.error(
                        f"❌ Error HTTPConnectionPool saat mengambil listing URLs: {e}. "
                        f"Mencoba lagi ({attempt}/{max_retries})..."
                    )
                    self.quit_driver()
                    time.sleep(5)
                    self.init_driver()
                else:
                    logging.error(f"❌ Error lain saat mengambil listing URLs: {e}. Tidak dilakukan retry.")
                    self.debug_dump("get_listing_urls_error")
                    return []
        return []

    def get_total_listing_count(self, base_url):
        try:
            url = re.sub(r"(page_number=)\d+", r"\g<1>1", base_url)
            self.driver.get(url)
            time.sleep(3)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#classified-listings-result > div.masthead.push--bottom > div > h1"))
                )
            except Exception as e:
                logging.warning("Elemen total listing tidak muncul tepat waktu.")

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            h1 = soup.select_one("#classified-listings-result > div.masthead.push--bottom > div > h1")
            if h1:
                text = h1.get_text()
                match = re.search(r"([\d,]+)\s+vehicles", text)
                if match:
                    count = int(match.group(1).replace(",", ""))
                    logging.info(f"📊 Total listing untuk brand ditemukan: {count}")
                    return count
            logging.warning("❌ Tidak bisa menemukan teks jumlah listing dalam h1.")
            return 0
        except Exception as e:
            logging.error(f"❌ Error mengambil total listing: {e}")
            return 0

    def save_scraping_progress(self, brand, last_page, total_scraped):
        status_file = "scraping_progress.json"
        progress = {}

        # Jika file sudah ada, baca dulu
        if os.path.exists(status_file):
            with open(status_file, "r") as f:
                try:
                    progress = json.load(f)
                except:
                    progress = {}

        progress[brand] = {
            "last_page": last_page,
            "total_scraped": total_scraped,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        with open(status_file, "w") as f:
            json.dump(progress, f, indent=4)

        logging.info(f"📌 Progress scraping {brand} disimpan.")

    def scrape_all_brands(self, start_brand=None, start_page=1):
        try:
            self.reset_scraping()
            df = pd.read_csv(INPUT_FILE)
            start_scraping = False if start_brand else True

            for _, row in df.iterrows():
                brand = row["brand"]
                base_url = row["url"]

                if not start_scraping:
                    if brand.lower() == start_brand.lower():
                        start_scraping = True
                        page = start_page
                    else:
                        continue
                else:
                    page = 1

                total_scraped = 0
                logging.info(f"🚀 Mulai scraping brand: {brand} dari halaman {page}")

                while not self.stop_flag:
                    paginated_url = re.sub(r"(page_number=)\d+", lambda m: f"{m.group(1)}{page}", base_url)
                    logging.info(f"📄 Scraping halaman {page}: {paginated_url}")

                    # Reinit browser dengan proxy baru setiap halaman
                    self.quit_driver()
                    time.sleep(random.uniform(2, 5))

                    if proxies:
                        self.active_proxy = random.choice(proxies)
                        logging.info(f"🌐 Proxy yang dipilih: {self.active_proxy[0]}:{self.active_proxy[1]}")
                        self.init_driver(self.active_proxy)
                    else:
                        self.active_proxy = None
                        self.init_driver()

                    self.get_current_ip()

                    try:
                        self.driver.get(paginated_url)
                        time.sleep(5)
                    except Exception as e:
                        logging.warning(f"❌ Gagal memuat halaman {paginated_url}: {e}")
                        take_screenshot(self.driver, f"page_load_error_{brand}_{page}")
                        break

                    if page == 1:
                        total_target = self.get_total_listing_count(base_url)
                        logging.info(f"📊 Total listing untuk brand {brand}: {total_target}")

                    html = self.driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    link_tags = soup.select("a.ellipsize.js-ellipsize-text")
                    urls = [tag.get("href") for tag in link_tags if tag.get("href")]

                    if not urls:
                        logging.warning(f"📄 Tidak ditemukan listing URL di halaman {page}")
                        take_screenshot(self.driver, f"no_listing_page_{page}_{brand}")
                        break

                    for url in urls:
                        if self.stop_flag:
                            break
                        logging.info(f"🔍 Scraping detail: {url}")
                        detail = self.scrape_detail(url)
                        if detail:
                            self.save_to_db(detail)

                        random_delay = random.randint(15, 20)
                        logging.info(f"🕒 Menunggu {random_delay} detik setelah scraping detail...")
                        time.sleep(random_delay)

                    total_scraped += len(urls)
                    self.save_scraping_progress(brand, page, total_scraped)

                    # Jika sudah mencapai kelipatan 1000, jeda panjang
                    if total_scraped > 0 and total_scraped % 1000 == 0:
                        delay = random.randint(3600, 7200)
                        logging.info(f"🛑 Telah mencapai {total_scraped} listing untuk brand {brand}, jeda selama {delay // 60} menit...")
                        time.sleep(delay)

                    page += 1

                # Jeda antar brand
                if not self.stop_flag:
                    delay_brand = random.randint(900, 1800)
                    logging.info(f"🛑 Selesai scraping brand {brand}, jeda {delay_brand // 60} menit sebelum lanjut brand berikutnya...")
                    time.sleep(delay_brand)

            logging.info("✅ Semua brand telah selesai diproses.")
        except Exception as e:
            logging.error(f"❌ Error saat scraping semua brand: {e}")
        finally:
            self.quit_driver()

    def stop_scraping(self):
        self.stop_flag = True
        logging.info("🛑 Scraping dihentikan oleh user.")

    def reset_scraping(self):
        self.stop_flag = False
        logging.info("🔄 Scraping direset dan siap dimulai kembali.")

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
                            last_scraped_at = CURRENT_TIMESTAMP
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        car_data['price'], car_data['informasi_iklan'], car_data['lokasi'], car_data['year'],
                        car_data['millage'], car_data['transmission'], car_data['seat_capacity'],
                        car_data['gambar'], car_data['listing_url']
                    ))
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

    def sync_to_cars(self):
        """
        Method untuk sync data dari tabel DB_TABLE_SCRAP ke DB_TABLE_PRIMARY.
        Jika listing_url sudah ada di DB_TABLE_PRIMARY, lakukan UPDATE.
        Jika belum ada, lakukan INSERT.
        """
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
            idx_info_iklan = col_names.index("informasi_iklan")
            idx_lokasi = col_names.index("lokasi")
            idx_price = col_names.index("price")
            idx_year = col_names.index("year")
            idx_millage = col_names.index("millage")
            idx_transmission = col_names.index("transmission")
            idx_seat_capacity = col_names.index("seat_capacity")
            idx_gambar = col_names.index("gambar")
            idx_last_scraped_at = col_names.index("last_scraped_at")
            idx_version = col_names.index("version")
            idx_created_at = col_names.index("created_at")

            for row in rows:
                listing_url = row[idx_url]

                # Cek apakah listing_url sudah ada di tabel DB_TABLE_PRIMARY
                check_query = f"SELECT id FROM {DB_TABLE_PRIMARY} WHERE listing_url = %s"
                self.cursor.execute(check_query, (listing_url,))
                result = self.cursor.fetchone()

                if result:
                    # UPDATE
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
                            last_scraped_at = %s,
                            version = %s,
                            created_at = %s
                        WHERE listing_url = %s
                    """
                    self.cursor.execute(update_query, (
                        row[idx_brand],
                        row[idx_model],
                        row[idx_variant],
                        row[idx_info_iklan],
                        row[idx_lokasi],
                        row[idx_price],
                        row[idx_year],
                        row[idx_millage],
                        row[idx_transmission],
                        row[idx_seat_capacity],
                        row[idx_gambar],
                        row[idx_last_scraped_at],
                        row[idx_version],
                        row[idx_created_at],
                        listing_url,
                    ))
                else:
                    # INSERT
                    insert_query = f"""
                        INSERT INTO {DB_TABLE_PRIMARY}
                            (listing_url, brand, model, variant, informasi_iklan, lokasi,
                             price, year, millage, transmission, seat_capacity, gambar, last_scraped_at, version, created_at)
                        VALUES
                            (%s, %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    self.cursor.execute(insert_query, (
                        listing_url,
                        row[idx_brand],
                        row[idx_model],
                        row[idx_variant],
                        row[idx_info_iklan],
                        row[idx_lokasi],
                        row[idx_price],
                        row[idx_year],
                        row[idx_millage],
                        row[idx_transmission],
                        row[idx_seat_capacity],
                        row[idx_gambar],
                        row[idx_last_scraped_at],
                        row[idx_version],
                        row[idx_created_at]
                    ))

            self.conn.commit()
            logging.info(f"Sinkronisasi data dari {DB_TABLE_SCRAP} ke {DB_TABLE_PRIMARY} selesai.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"❌ Error saat sinkronisasi data: {e}")

    def close(self):
        """Menutup driver dan koneksi database."""
        self.quit_driver()
        self.cursor.close()
        self.conn.close()
        logging.info("Koneksi database ditutup, driver Selenium ditutup.")
