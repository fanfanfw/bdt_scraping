from flask import Flask, jsonify
from scrap_service.listing_tracker_service_mudahmy.listing_tracker_mudahmy import ListingTrackerMudahmy

app = Flask(__name__)
tracker = ListingTrackerMudahmy()

@app.route('/track/listings/mudahmy', methods=['POST'])
def track_listings():
    """
    Endpoint untuk memeriksa status iklan di database.
    """
    tracker.track_listings()
    return jsonify({"message": "Proses tracking iklan dimulai"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5005, debug=True)
