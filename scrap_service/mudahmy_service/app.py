from flask import Flask, jsonify, request
from scrap_service.mudahmy_service.mudahmy_service import MudahMyService
import psycopg2

app = Flask(__name__)
mudahmy_scraper = MudahMyService()

@app.route('/scrape/mudahmy', methods=['POST'])
def scrape_mudahmy():
    data = request.get_json()  
    brand = data.get("brand", None)  
    page = data.get("page", 1)  
    
    mudahmy_scraper.stop_flag = False
    mudahmy_scraper.scrape_all_brands(start_brand=brand, start_page=page)
    
    return jsonify({"message": "Scraping MudahMY selesai"}), 200


@app.route('/stop/mudahmy', methods=['POST'])
def stop_mudahmy():
    mudahmy_scraper.stop_flag = True  
    return jsonify({"message": "Scraping MudahMY dihentikan."}), 200

# Fungsi untuk mengambil data dari database PostgreSQL
def fetch_latest_data():
    conn = psycopg2.connect(
        dbname="scrap_mudahmy",
        user="fanfan",
        password="cenanun",
        host="localhost",
        port="5432"  
    )
    
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM cars;")

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
    app.run(host="0.0.0.0", port=5001, debug=True)
