from flask import Flask, jsonify, request
from scrap_service.listing_tracker_service_carlistmy_playwright.listing_tracker_carlistmy_playwright import ListingTrackerCarlistmyPlaywright

app = Flask(__name__)
tracker = ListingTrackerCarlistmyPlaywright()

@app.route('/track/listings/carlistmy', methods=['POST'])
def track_listings():
    data = request.get_json()
    start_id = data.get("id", 1)
    tracker.track_listings(start_id=start_id)
    return jsonify({"message": f"Proses tracking iklan dimulai dari ID {start_id}"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5004, debug=True)
