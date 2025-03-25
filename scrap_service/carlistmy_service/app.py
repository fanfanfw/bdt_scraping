from flask import Flask, jsonify, request
from scrap_service.carlistmy_service.carlistmy_service import CarlistMyService
import psycopg2
import os

app = Flask(__name__)
carlistmy_scraper = CarlistMyService()

# Ambil nama tabel dari environment (atau pakai default jika tidak ada).
DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")
DB_TABLE_PRIMARY = os.getenv("DB_TABLE_PRIMARY", "cars")

@app.route('/scrape/carlistmy', methods=['POST'])
def scrape_carlistmy():
    data = request.get_json()
    brand = data.get("brand", None)
    page = data.get("page", 1)

    carlistmy_scraper.stop_flag = False
    carlistmy_scraper.scrape_all_brands(start_brand=brand, start_page=page)

    return jsonify({"message": "Scraping CarlistMY selesai"}), 200

@app.route('/stop/carlistmy', methods=['POST'])
def stop_carlistmy():
    carlistmy_scraper.stop_flag = True
    return jsonify({"message": "Scraping CarlistMY dihentikan."}), 200

def fetch_latest_data():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()
    # Gunakan nama tabel dari environment
    query = f"SELECT * FROM {DB_TABLE_SCRAP};"
    cursor.execute(query)
    rows = cursor.fetchall()

    column_names = [desc[0] for desc in cursor.description]
    data = [dict(zip(column_names, row)) for row in rows]

    cursor.close()
    conn.close()
    return data

@app.route('/export_data', methods=['GET'])
def export_data():
    data = fetch_latest_data()
    return jsonify(data)

@app.route('/sync_to_cars', methods=['POST'])
def sync_to_cars():
    """
    Melakukan sinkronisasi data dari tabel cars_scrap ke tabel cars.
    Jika listing_url sudah ada di tabel cars, maka update.
    Jika belum ada, insert data baru.
    """
    try:
        carlistmy_scraper.sync_to_cars()
        return jsonify({"message": "Sinkronisasi data dari cars_scrap ke cars berhasil."}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True)
