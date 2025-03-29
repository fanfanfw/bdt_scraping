# mudahmy_service_playwright/app.py

from flask import Flask, jsonify, request
from scrap_service.mudahmy_service_playwright.mudahmy_service import MudahMyService
from scrap_service.mudahmy_service_playwright.database import get_connection
import os
import psycopg2

app = Flask(__name__)

# Inisialisasi instance service
mudahmy_scraper = MudahMyService()

DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "url")

@app.route('/scrape/mudahmy', methods=['POST'])
def scrape_mudahmy():
    data = request.get_json()
    brand = data.get("brand", None)
    model = data.get("model", None)
    page = data.get("page", 1)  

    mudahmy_scraper.stop_flag = False

    # Panggil fungsi scraping dengan filter brand, model, dan halaman
    mudahmy_scraper.scrape_all_brands(brand=brand, model=model, start_page=page)

    return jsonify({"message": "Scraping mudahMY selesai"}), 200

@app.route('/stop/mudahmy', methods=['POST'])
def stop_mudahmy():
    mudahmy_scraper.stop_scraping()
    return jsonify({"message": "Scraping mudahMY dihentikan."}), 200

def fetch_latest_data():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()
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
    try:
        mudahmy_scraper.sync_to_cars()
        return jsonify({"message": "Sinkronisasi data dari scrap ke primary berhasil."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True)
