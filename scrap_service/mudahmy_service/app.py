from flask import Flask, jsonify
from scrap_service.mudahmy_service.mudahmy_service import MudahMyService

app = Flask(__name__)
mudahmy_scraper = MudahMyService()

@app.route('/scrape/mudahmy', methods=['POST'])
def scrape_carlistmy():
    mudahmy_scraper.stop_flag = False
    mudahmy_scraper.scrape_all_brands()
    return jsonify({"message": "Scraping CarlistMY selesai"}), 200


@app.route('/stop/mudahmy', methods=['POST'])
def stop_mudahmy():
    mudahmy_scraper.stop_flag = True  
    return jsonify({"message": "Scraping MudahMY dihentikan."}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5001, debug=True)
