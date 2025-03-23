import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from .database import get_database_connection

logger = logging.getLogger("carlistmy_tracker")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s') 
console_handler.setFormatter(formatter)

logger.handlers = []
logger.addHandler(console_handler)

class ListingTrackerCarlistmy:
    def __init__(self, batch_size=10):
        self.batch_size = batch_size

        self.sold_selector = (
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs."
            "u-flex\\@mobile.u-flex--column\\@mobile > div > div > div > h2"
        )
        self.sold_text_indicator = "This car has already been sold."

        self.active_selector = (
            "#listing-detail > section.c-section--content.u-bg-white.u-padding-top-md.u-padding-bottom-xs."
            "u-flex\\@mobile.u-flex--column\\@mobile > section.c-section.c-section--masthead.u-margin-ends-lg."
            "u-margin-ends-sm\\@mobile.u-order-2\\@mobile > div > h1"
        )

    def _init_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/110.0.5481.77 Safari/537.36"
        )
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
        driver.set_page_load_timeout(30)
        return driver

    def _close_cookies_popup(self, driver):
        try:
            popup_selector = ".cmm-cookie-acceptAll"
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, popup_selector))
            ).click()
            time.sleep(1)
        except:
            pass
        
    def _update_car_info(self, car_id, status, sold_at):
        """
        Memperbarui status mobil di database (tabel 'cars').
        """
        conn = None
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            query = """
                UPDATE cars
                SET status = %s, sold_at = %s, last_scraped_at = %s
                WHERE id = %s
            """
            cursor.execute(query, (status, sold_at, datetime.now(), car_id))
            conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Error updating car info for car_id={car_id}: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def _process_listing(self, driver, car_id, listing_url):
        logger.info(f"> ID={car_id} => Memeriksa {listing_url}")
        try:
            driver.get(listing_url)
            time.sleep(3)
            
            self._close_cookies_popup(driver)
            
            driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(3)
            
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.sold_selector))
                )
                found_text = driver.find_element(By.CSS_SELECTOR, self.sold_selector).text.strip()
                if self.sold_text_indicator in found_text:
                    logger.info(f"> ID={car_id} => terdeteksi SOLD (\"{found_text}\")")
                    self._update_car_info(car_id, "sold", datetime.now())
                    return
            except:
                pass
            
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, self.active_selector))
                )
                logger.info(f"> ID={car_id} => masih active (judul iklan ditemukan)")
                return
            except:
                pass
            
            page_source = driver.page_source.lower()
            if "this car has already been sold." in page_source:
                logger.info(f"> ID={car_id} => terdeteksi SOLD (fallback by page_source)")
                self._update_car_info(car_id, "sold", datetime.now())
            else:
                # Misal kita tandai 'unknown' agar tidak salah
                logger.info(f"> ID={car_id} => fallback => tidak ditemukan sold, tandai 'unknown'")
                # self._update_car_info(car_id, "unknown", None)

            # (Jika Anda ingin langsung sold, boleh)
            # else:
            #     logger.info(f"> ID={car_id} => tidak ada indikasi active, dianggap SOLD.")
            #     self._update_car_info(car_id, "sold", datetime.now())

        except Exception as e:
            logger.error(f"Error processing listing ID={car_id}, URL={listing_url}: {e}")
            self._update_car_info(car_id, "sold", datetime.now())


    def track_listings(self):
        """
        Fungsi utama: mengambil semua listing 'active', memprosesnya batch per batch.
        """
        conn = None
        try:
            conn = get_database_connection()
            cursor = conn.cursor()

            # Ambil SEMUA data yg status='active'
            cursor.execute("SELECT id, listing_url FROM cars WHERE status = 'active' ORDER BY id")
            all_records = cursor.fetchall()
            cursor.close()

            total_active = len(all_records)
            logger.info(f"Total listing aktif: {total_active}")
            if total_active == 0:
                return

            # Bagi data menjadi batch
            start_index = 0
            batch_number = 0

            while start_index < total_active:
                end_index = start_index + self.batch_size
                batch_records = all_records[start_index:end_index]
                batch_number += 1

                logger.info(f"\nMemproses batch ke-{batch_number} (index {start_index} s/d {end_index-1}), jumlah={len(batch_records)}...")

                # Inisialisasi driver untuk batch ini
                driver = self._init_driver()

                try:
                    for record in batch_records:
                        car_id, listing_url = record
                        self._process_listing(driver, car_id, listing_url)
                finally:
                    driver.quit()

                start_index = end_index

        except Exception as e:
            logger.error(f"Error in track_listings: {e}")
        finally:
            if conn:
                conn.close()
            logger.info("Proses tracking selesai.")
