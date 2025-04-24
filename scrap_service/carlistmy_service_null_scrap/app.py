from flask import Flask, jsonify, request
from carlistmy_null_service import CarlistMyNullService

app = Flask(__name__)
null_scraper = CarlistMyNullService()

@app.route('/scrape_null', methods=['POST'])
def scrape_null_entries():
    null_scraper.stop_flag = False
    null_scraper.scrape_null_entries()
    return jsonify({"message": "Scraping data null selesai."}), 200

@app.route('/stop_null', methods=['POST'])
def stop_scraping():
    null_scraper.stop_flag = True
    return jsonify({"message": "Scraping data null dihentikan."}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5003, debug=True)