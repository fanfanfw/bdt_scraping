import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from scrap_service.listing_tracker_service_carlistmy_playwright.database import get_database_connection

load_dotenv()

# Konfigurasi logging
START_DATE = datetime.now().strftime('%Y%m%d')
log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"update_location_{START_DATE}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("update_location_service")

class UpdateLocationService:
    def __init__(self):
        self.conn = get_database_connection()
        if not self.conn:
            logger.error("❌ Gagal koneksi ke database.")
            raise Exception("Database connection failed")
        self.cursor = self.conn.cursor()

    def fetch_location_data(self, listing_url):
        """Mengambil data lokasi dari halaman listing."""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                page.goto(listing_url, timeout=60000)

                # Contoh: Ambil data lokasi dari elemen tertentu
                location_selector = "div.location"
                location = page.locator(location_selector).inner_text().strip()

                browser.close()
                return location
        except Exception as e:
            logger.error(f"❌ Gagal mengambil data lokasi dari {listing_url}: {e}")
            return None

    def update_location_in_db(self, listing_url, location):
        """Memperbarui data lokasi di database berdasarkan listing_url."""
        try:
            now = datetime.now()
            self.cursor.execute(f"""
                UPDATE {os.getenv('DB_TABLE_PRIMARY', 'cars')}
                SET location = %s, last_updated_at = %s
                WHERE listing_url = %s
            """, (location, now, listing_url))
            self.conn.commit()
            logger.info(f"✅ Lokasi untuk {listing_url} berhasil diperbarui ke '{location}'")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Gagal memperbarui lokasi untuk {listing_url}: {e}")

    def fetch_listings_with_authorized_location(self):
        """Mengambil daftar listing dengan lokasi yang mengandung 'Authorized'."""
        try:
            self.cursor.execute(f"""
                SELECT id, listing_url, lokasi
                FROM {os.getenv('DB_TABLE_PRIMARY', 'dashboard_carscarlistmy')}
                WHERE lokasi LIKE %s
            """, ('%Authorized%',))
            rows = self.cursor.fetchall()
            logger.info(f"✅ Ditemukan {len(rows)} listing dengan lokasi 'Authorized'.")
            return rows
        except Exception as e:
            logger.error(f"❌ Gagal mengambil listing dengan lokasi 'Authorized': {e}")
            return []

    def process_listings(self, listing_urls):
        """Proses daftar listing untuk memperbarui lokasi."""
        for listing_url in listing_urls:
            logger.info(f"🔍 Memproses {listing_url}...")
            location = self.fetch_location_data(listing_url)
            if location:
                self.update_location_in_db(listing_url, location)

    def process_authorized_listings(self):
        """Proses semua listing dengan lokasi 'Authorized' untuk diperbarui."""
        listings = self.fetch_listings_with_authorized_location()
        for listing_id, listing_url, current_location in listings:
            logger.info(f"🔍 Memproses ID={listing_id}, URL={listing_url}, Lokasi Saat Ini='{current_location}'...")
            location = self.fetch_location_data(listing_url)
            if location:
                self.update_location_in_db(listing_url, location)

    def close(self):
        """Menutup koneksi database."""
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logger.error(f"❌ Gagal menutup koneksi database: {e}")

if __name__ == "__main__":
    # Contoh penggunaan
    service = UpdateLocationService()
    try:
        # Daftar URL listing yang ingin diproses
        listing_urls = [
            "https://example.com/listing/1",
            "https://example.com/listing/2",
        ]
        service.process_listings(listing_urls)
    finally:
        service.close()
