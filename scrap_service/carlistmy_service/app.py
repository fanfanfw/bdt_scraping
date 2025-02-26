from flask import Flask, jsonify
from scrap_service.carlistmy_service.carlistmy_service import CarlistMyService


app = Flask(__name__)
carlistmy_scraper = CarlistMyService()

@app.route('/scrape/carlistmy', methods=['POST'])
def scrape_carlistmy():
    carlistmy_scraper.stop_flag = False
    carlistmy_scraper.scrape_all_brands()
    return jsonify({"message": "Scraping CarlistMY selesai"}), 200

@app.route('/stop/carlistmy', methods=['POST'])
def stop_carlistmy():
    carlistmy_scraper.stop_flag = True  # Set stop_flag menjadi True untuk menghentikan scraping
    return jsonify({"message": "Scraping CarlistMY dihentikan."}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5002, debug=True)
