import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scrap_service.listing_tracker_service_mudahmy.database import get_database_connection

logger = logging.getLogger("mudahmy_tracker")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()  
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)

logger.handlers = []
logger.addHandler(console_handler)

class ListingTrackerMudahmy:
    def __init__(self, batch_size=10):
        """
        batch_size = jumlah data yang diproses per satu kali inisiasi driver
        sebelum driver di-quit, lalu batch berikutnya dibuka driver baru.
        """
        self.batch_size = batch_size
        self.redirect_url = "https://www.mudah.my/malaysia/cars-for-sale"

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

    def _is_redirected(self, driver):
        """
        Cek apakah URL saat ini mengarah ke halaman 'cars-for-sale' (menandakan sold).
        """
        return driver.current_url.startswith(self.redirect_url)

    def _check_h1_active(self, driver):
        """
        Mengecek apakah ada elemen H1 di halaman iklan.
        Jika ditemukan, kita anggap masih active.
        """
        try:
            driver.execute_script("window.scrollTo(0, 300);")
            time.sleep(2)

            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#ad_view_ad_highlights > div > div > h1"))
            )
            return True
        except:
            return False

    def track_listings(self):
        conn = get_database_connection()
        if not conn:
            logger.error("Koneksi DB gagal, hentikan proses tracking.")
            return

        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, listing_url
            FROM cars_mudahmy
            WHERE status = 'active'
            ORDER BY id
        """)
        all_records = cursor.fetchall()

        total_active = len(all_records)
        logger.info(f"Total listing aktif: {total_active}")

        start_index = 0
        batch_number = 0

        while start_index < total_active:
            end_index = start_index + self.batch_size
            batch_records = all_records[start_index:end_index]
            batch_number += 1

            logger.info(f"\nMemproses batch ke-{batch_number} (index {start_index} s/d {end_index-1}), "
                        f"jumlah={len(batch_records)}...")

            driver = self._init_driver()

            try:
                for row in batch_records:
                    car_id, url = row
                    # LOG URL
                    logger.info(f"> ID={car_id} => Memeriksa {url}")

                    try:
                        driver.get(url)
                        time.sleep(3)

                        if self._is_redirected(driver):
                            logger.info(f"> ID={car_id} => redirect -> 'sold'")
                            cursor.execute("""
                                UPDATE cars_mudahmy
                                SET status = 'sold', sold_at = NOW()
                                WHERE id = %s
                            """, (car_id,))
                        else:
                            if self._check_h1_active(driver):
                                logger.info(f"> ID={car_id} => masih active (H1 ditemukan)")
                            else:
                                logger.info(f"> ID={car_id} => tidak ada H1 -> 'sold'")
                                driver.save_screenshot(f"screenshot_{car_id}.png")
                                with open(f"page_source_{car_id}.html", "w", encoding="utf-8") as f:
                                    f.write(driver.page_source)

                                cursor.execute("""
                                    UPDATE cars_mudahmy
                                    SET status = 'sold', sold_at = NOW()
                                    WHERE id = %s
                                """, (car_id,))

                        conn.commit()

                    except Exception as e:
                        logger.error(f"Terjadi error saat cek ID={car_id}: {e}")
                        cursor.execute("""
                            UPDATE cars_mudahmy
                            SET status = 'sold', sold_at = NOW()
                            WHERE id = %s
                        """, (car_id,))
                        conn.commit()

            finally:
                driver.quit()

            start_index = end_index

        cursor.close()
        conn.close()
        logger.info("\nProses tracking selesai.")
