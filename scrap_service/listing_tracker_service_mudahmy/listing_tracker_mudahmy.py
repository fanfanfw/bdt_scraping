import time
import logging
from datetime import datetime
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
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
        """
        Inisialisasi ChromeDriver dengan berbagai opsi agar lebih cepat & mengurangi deteksi bot.
        """
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

        chrome_options.page_load_strategy = 'eager'

        driver = webdriver.Chrome(options=chrome_options)

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })

        driver.set_page_load_timeout(60)
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

    def random_delay(self, min_delay=3, max_delay=10):
        """ Menambahkan delay acak antara permintaan """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def get_page_with_retry(self, driver, url, retries=3, delay=5):
        """ Mencoba beberapa kali untuk memuat halaman dengan retry mechanism """
        for attempt in range(retries):
            try:
                driver.get(url)
                return True
            except (TimeoutException, WebDriverException) as e:
                logger.error(f"Attempt {attempt + 1} gagal: {e}")
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} detik...")
                    time.sleep(delay)
                else:
                    logger.error(f"Gagal memuat {url} setelah {retries} percobaan.")
                    return False

    def track_listings(self, start_id=1):
        conn = get_database_connection()
        if not conn:
            logger.error("Koneksi DB gagal, hentikan proses tracking.")
            return

        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, listing_url
            FROM cars
            WHERE status = 'active' AND id >= %s
            ORDER BY id
        """, (start_id,))
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
                driver.get("https://ifconfig.me")
                time.sleep(2)
                ip_text = driver.find_element(By.TAG_NAME, "body").text
                logger.info(f"IP yang terdeteksi oleh Selenium: {ip_text}")
            except Exception as e:
                logger.error(f"Gagal cek IP: {e}")

            try:
                for row in batch_records:
                    car_id, url = row
                    logger.info(f"> ID={car_id} => Memeriksa {url}")

                    if not self.get_page_with_retry(driver, url):
                        cursor.execute("""
                            UPDATE cars
                            SET status = 'sold', sold_at = NOW()
                            WHERE id = %s
                        """, (car_id,))
                        conn.commit()
                        continue

                    self.random_delay(3, 7)  

                    if self._is_redirected(driver):
                        logger.info(f"> ID={car_id} => redirect -> 'sold'")
                        cursor.execute("""
                            UPDATE cars
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
                                UPDATE cars
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