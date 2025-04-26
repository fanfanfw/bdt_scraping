import time
import logging
import random
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from .database import get_database_connection

logger = logging.getLogger("carlistmy_tracker")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s') 
console_handler.setFormatter(formatter)

logger.handlers = []
logger.addHandler(console_handler)

DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")

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
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
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
            query = f"""
                UPDATE {DB_TABLE_PRIMARY}
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

    def track_listings(self, start_id=1):
        """
        Fungsi utama: mengambil semua listing berstatus 'active' maupun 'unknown'
        mulai dari ID tertentu, lalu memprosesnya batch per batch.
        """
        conn = None
        try:
            conn = get_database_connection()
            cursor = conn.cursor()

            # Ambil SEMUA data yg status='active' atau 'unknown' mulai dari ID yang ditentukan
            cursor.execute(f"""
                SELECT id, listing_url, status
                FROM {DB_TABLE_PRIMARY}
                WHERE (status = 'active' OR status = 'unknown')
                AND id >= %s
                ORDER BY id
            """, (start_id,))
            all_records = cursor.fetchall()
            cursor.close()

            total_listings = len(all_records)
            unknown_count = sum(1 for r in all_records if r[2] == 'unknown')

            logger.info(f"Total listing berstatus active/unknown (mulai ID {start_id}): {total_listings}")
            logger.info(f"Di antaranya, ada {unknown_count} listing dengan status 'unknown'.")

            if total_listings == 0:
                return

            start_index = 0
            batch_number = 0

            # Bagi data menjadi batch
            while start_index < total_listings:
                end_index = start_index + self.batch_size
                batch_records = all_records[start_index:end_index]
                batch_number += 1

                logger.info(f"\nMemproses batch ke-{batch_number} (index {start_index} s/d {end_index-1}), "
                            f"jumlah={len(batch_records)}...")

                driver = self._init_driver()
                try:
                    for record in batch_records:
                        car_id, listing_url, current_status = record  # <-- tambahan
                        self._process_listing_with_retry(driver, car_id, listing_url, current_status)
                finally:
                    driver.quit()

                start_index = end_index

        except Exception as e:
            logger.error(f"Error in track_listings: {e}")
        finally:
            if conn:
                conn.close()
            logger.info("Proses tracking selesai.")


    def _process_listing_with_retry(self, driver, car_id, listing_url, current_status, max_retries=3):
        """
        Mencoba memproses listing hingga max_retries kali jika terjadi exception.
        Jika setelah max_retries kali tetap error, skip listing ini.
        """
        for attempt in range(max_retries):
            try:
                self._process_listing(driver, car_id, listing_url, current_status)  # <-- tambahan
                return True
            except (TimeoutException, WebDriverException, Exception) as e:
                logger.error(f"Attempt {attempt+1} gagal memproses ID={car_id}, URL={listing_url}: {e}")
                if attempt < max_retries - 1:
                    logger.info("Menunggu beberapa detik sebelum retry berikutnya...")
                    time.sleep(3)
                else:
                    logger.error(f"Gagal memproses ID={car_id} setelah {max_retries} kali percobaan. Skip ID ini.")
                    return False


    def _process_listing(self, driver, car_id, listing_url, current_status):  # <-- tambahan
        """
        Proses inti untuk membuka URL dan menentukan apakah listing 'sold' atau 'active'.
        Apabila terjadi error di sini, di-raise ke atas agar bisa ditangani oleh mekanisme retry.
        """
        logger.info(f"> ID={car_id} => Memeriksa {listing_url}")

        driver.get(listing_url)
        time.sleep(3)

        self._close_cookies_popup(driver)
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(3)

        # Cek apakah ada indikator 'sold'
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

        # Cek apakah masih active
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.active_selector))
            )
            logger.info(f"> ID={car_id} => masih active (judul iklan ditemukan)")
            # Jika awalnya unknown, update jadi active
            if current_status == "unknown":
                self._update_car_info(car_id, "active", None)
            return
        except:
            pass

        page_source = driver.page_source.lower()
        if "this car has already been sold." in page_source:
            logger.info(f"> ID={car_id} => terdeteksi SOLD (fallback by page_source)")
            self._update_car_info(car_id, "sold", datetime.now())
        else:
            # Jika sudah unknown, biarkan tetap unknown
            if current_status == "unknown":
                logger.info(f"> ID={car_id} => fallback => status sudah unknown, biarkan tetap unknown.")
            else:
                logger.info(f"> ID={car_id} => fallback => tidak ditemukan sold, tandai 'unknown'")
                self._update_car_info(car_id, "unknown", None)

