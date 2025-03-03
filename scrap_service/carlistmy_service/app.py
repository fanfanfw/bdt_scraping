from flask import Flask, jsonify, request
from scrap_service.carlistmy_service.carlistmy_service import CarlistMyService
import psycopg2

app = Flask(__name__)
carlistmy_scraper = CarlistMyService()

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

# Fungsi untuk mengambil data dari database PostgreSQL
def fetch_latest_data():
    conn = psycopg2.connect("dbname=scrap_mudahmy user=fanfan password=cenanun")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cars WHERE last_scraped_at >= NOW() - INTERVAL '6 hours';")
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

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True)