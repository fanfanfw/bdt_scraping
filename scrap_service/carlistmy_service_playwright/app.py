from flask import Flask, jsonify, request
from scrap_service.carlistmy_service_playwright.carlistmy_service import CarlistMyService
import psycopg2
import os

app = Flask(__name__)
carlistmy_scraper = CarlistMyService()

DB_TABLE_SCRAP = os.getenv("DB_TABLE_SCRAP", "cars_scrap")

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
    carlistmy_scraper.stop_scraping()
    return jsonify({"message": "Scraping CarlistMY dihentikan."}), 200

@app.route('/export_data', methods=['GET'])
def export_data():
    try:
        data = carlistmy_scraper.export_data()
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync_to_cars', methods=['POST'])
def sync_to_cars():
    try:
        carlistmy_scraper.sync_to_cars()
        return jsonify({"message": "Sinkronisasi berhasil"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True)
