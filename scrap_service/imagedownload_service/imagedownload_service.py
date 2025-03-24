import os
import json
import logging
import requests
from .database import get_database_connection

# Konfigurasi logging agar log tampil di terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Direktori penyimpanan gambar
SAVE_DIR = "scrap_service/imagedownload_service/storage/images"
os.makedirs(SAVE_DIR, exist_ok=True)

class ImageDownloadService:
    def __init__(self):
        self.conn = get_database_connection()

    def download_image(self, url, listing_id, index):
        """Fungsi untuk mendownload gambar dari URL."""
        try:
            response = requests.get(url, stream=True, timeout=10)
            if response.status_code == 200:
                filename = f"{listing_id}_{index+1}.jpg"
                image_path = os.path.join(SAVE_DIR, filename)
                with open(image_path, "wb") as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                logging.info(f"✅ Gambar berhasil diunduh: {filename}")
            else:
                logging.warning(f"⚠️ Gagal mengunduh {url} - Status: {response.status_code}")
        except Exception as e:
            logging.error(f"❌ Error saat mengunduh {url}: {e}")

    def run(self):
        """Mengambil semua gambar dari tabel `cars` secara berurutan berdasarkan ID."""
        logging.info("🚀 Memulai proses download gambar dari database...")

        cursor = self.conn.cursor()
        
        # Mengambil data dari tabel `cars` secara berurutan berdasarkan ID
        query = "SELECT id, gambar FROM cars ORDER BY id ASC"
        cursor.execute(query)

        for row in cursor.fetchall():
            listing_id, images_str = row

            # Menyesuaikan jika data dalam kolom `gambar` sudah dalam format list atau masih string JSON
            if isinstance(images_str, str):
                try:
                    images = json.loads(images_str)
                except json.JSONDecodeError:
                    logging.error(f"❌ Error parsing JSON gambar untuk listing ID: {listing_id}")
                    continue
            else:
                images = images_str  # Jika sudah berbentuk list

            for idx, img_url in enumerate(images):
                self.download_image(img_url, listing_id, idx)

        cursor.close()
        self.conn.close()
        logging.info("✅ Semua gambar telah diproses secara berurutan berdasarkan ID.")
