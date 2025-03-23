import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .database import get_database_connection
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ListingTrackerMudahmy:
    def __init__(self):
        self.driver = None

    def _init_driver(self):
        """
        Menginisialisasi dan mengonfigurasi driver Selenium.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Menjalankan dalam mode headless
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        self.driver = webdriver.Chrome(options=chrome_options)
    
    def _close_driver(self):
        """
        Menutup driver jika sudah tidak digunakan.
        """
        if self.driver:
            self.driver.quit()
    
    def _process_listing(self, listing_url, car_id):
        """
        Fungsi untuk memproses setiap listing dan mengecek status iklan.
        """
        try:
            logger.info(f"Memproses URL: {listing_url}")
            self.driver.get(listing_url)

            # Cek apakah URL di-redirect ke halaman "cars-for-sale"
            if "https://www.mudah.my/malaysia/cars-for-sale" in self.driver.current_url:
                logger.info(f"Iklan di {listing_url} sudah tidak aktif (redirect ke halaman cars-for-sale).")
                new_status = "sold"
                sold_at = datetime.now()
                self._update_car_info(car_id, new_status, sold_at)
                return

            # Selektor untuk judul iklan aktif
            active_selector = "#ad_view_ad_highlights > div > div > h1"

            # Cek apakah judul iklan ditemukan
            try:
                WebDriverWait(self.driver, 30).until(  # Tambahkan waktu tunggu yang lebih lama
                    EC.presence_of_element_located((By.CSS_SELECTOR, active_selector))
                )
                logger.info(f"Iklan di {listing_url} masih aktif.")
            except Exception as e:
                logger.warning(f"Judul iklan tidak ditemukan di {listing_url}: {e}")
        
        except Exception as e:
            logger.error(f"Error processing {listing_url}: {e}")
            raise  # Re-raise the exception after logging

    def _update_car_info(self, car_id, status, sold_at):
        """
        Fungsi untuk memperbarui informasi mobil di tabel cars.
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
        except Exception as e:
            logger.error(f"Error updating car info for car_id {car_id}: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cursor.close()
                conn.close()

    def track_listings(self):
        """
        Fungsi utama untuk melacak iklan dan memprosesnya.
        """
        conn = None
        try:
            conn = get_database_connection()
            cursor = conn.cursor()

            # Ambil listing_url dari tabel cars
            cursor.execute("SELECT id, listing_url FROM cars")
            listings = cursor.fetchall()

            if len(listings) == 0:
                logger.info("Tidak ada iklan untuk diproses.")
                return

            # Inisialisasi driver
            self._init_driver()

            # Proses setiap listing
            for listing in listings:
                car_id, listing_url = listing
                self._process_listing(listing_url, car_id)

        except Exception as e:
            logger.error(f"Error in track_listings: {e}")
        finally:
            # Tutup koneksi dan driver setelah selesai
            self._close_driver()
            if conn:
                cursor.close()
                conn.close()

# Contoh penggunaan
if __name__ == "__main__":
    tracker = ListingTrackerMudahmy()
    tracker.track_listings()