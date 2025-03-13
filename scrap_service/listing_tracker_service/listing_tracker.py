import os
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from .database import get_database_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

class ListingTracker:
    def __init__(self):
        self.conn = get_database_connection()
        self.cursor = self.conn.cursor()
        self.driver = None
        self.batch_size = 10  
        self.listing_count = 0  

    def init_driver(self):
        logging.info("🟢 Menginisialisasi ChromeDriver...")
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
        self.driver.set_page_load_timeout(30)  
        logging.info("✅ ChromeDriver berhasil diinisialisasi.")

    def quit_driver(self):
        if self.driver:
            logging.info("🔴 Menutup ChromeDriver...")
            try:
                self.driver.quit()
                logging.info("✅ ChromeDriver berhasil ditutup.")
            except Exception as e:
                logging.error(f"⚠️ Gagal menutup ChromeDriver: {e}")
            self.driver = None

    def check_listing_carlist(self, listing_url):
        """Gunakan Selenium untuk mengecek status listing dari Carlist.my."""
        if not self.driver:
            self.init_driver()

        try:
            self.driver.get(listing_url)
            time.sleep(3)

            if "404" in self.driver.title or "Page not found" in self.driver.page_source:
                logging.info(f"❌ Iklan tidak ditemukan (404): {listing_url}")
                return False  

            if "Special offer - call now!" in self.driver.page_source:
                logging.warning(f"⚠️ Iklan masih ada, tetapi harganya kemungkinan hilang: {listing_url}")
                return "price_missing"  

            logging.info(f"✅ Iklan masih aktif: {listing_url}")
            return True  

        except Exception as e:
            logging.error(f"❌ Error saat mengakses {listing_url}: {e}")
            return None
        
    def check_listing_mudah(self, listing_url):
        """Gunakan Selenium untuk mengecek status listing dari Mudah.my."""
        if not self.driver:
            self.init_driver()

        try:
            self.driver.get(listing_url)
            time.sleep(3)

            if "cars-for-sale" in self.driver.current_url or "mudah.my/malaysia" in self.driver.current_url:
                logging.info(f"❌ Iklan sudah dihapus (redirect ke halaman utama): {listing_url}")
                return False  

            logging.info(f"✅ Iklan masih aktif: {listing_url}")
            return True  

        except Exception as e:
            logging.error(f"❌ Error saat mengakses {listing_url}: {e}")
            return None

    def track_listings(self):
        """Melakukan tracking hanya untuk listing yang masih aktif."""
        logging.info("🚀 Memulai tracking status iklan...")

        query = "SELECT id, listing_url, source FROM cars WHERE status_aktif = TRUE ORDER BY id ASC"
        self.cursor.execute(query)

        listings = self.cursor.fetchall()
        logging.info(f"🔍 Total listing yang akan diperiksa: {len(listings)}")

        for row in listings:
            listing_id, listing_url, source = row
            retry_count = 0  

            while retry_count < 3:  
                try:
                    if source == "carlistmy":
                        is_active = self.check_listing_carlist(listing_url)
                    elif source == "mudahmy":
                        is_active = self.check_listing_mudah(listing_url)
                    else:
                        logging.warning(f"⚠️ Sumber tidak dikenali: {source} untuk {listing_url}")
                        break 

                    if is_active is None:
                        logging.warning(f"⚠️ Error saat mengecek {listing_url}. Coba ulang (retry {retry_count+1}/3)")
                        retry_count += 1
                        continue  

                    if is_active is True:
                        update_query = "UPDATE cars SET last_checked_at = NOW() WHERE id = %s"
                        logging.info(f"✅ Iklan masih aktif: {listing_url}")

                    elif is_active == "price_missing":
                        update_query = "UPDATE cars SET last_checked_at = NOW(), price = NULL WHERE id = %s"
                        logging.warning(f"⚠️ Iklan masih ada, tetapi harga tidak ditemukan: {listing_url}")

                    else:
                        update_query = "UPDATE cars SET status_aktif = FALSE, last_checked_at = NOW(), ended_at = NOW() WHERE id = %s"
                        logging.info(f"❌ Iklan sudah dihapus: {listing_url}")

                    self.cursor.execute(update_query, (listing_id,))
                    self.conn.commit()
                    self.listing_count += 1
                    break  

                except Exception as e:
                    logging.error(f"❌ Error saat mengakses {listing_url}: {e}")
                    retry_count += 1

                    if retry_count >= 3:
                        logging.error(f"🚨 Gagal mengecek {listing_url} setelah 3 percobaan. Lewati ke berikutnya.")
                        break  

                    logging.info("♻️ Menutup dan me-restart ChromeDriver...")
                    self.quit_driver()
                    time.sleep(2)
                    self.init_driver()

            if self.listing_count >= self.batch_size:
                logging.info(f"♻️ Mencapai {self.batch_size} listing, restart ChromeDriver...")
                self.quit_driver()
                time.sleep(2)
                self.init_driver()
                self.listing_count = 0  

        self.cursor.close()
        self.conn.close()
        self.quit_driver()
        logging.info("✅ Tracking iklan selesai.")
