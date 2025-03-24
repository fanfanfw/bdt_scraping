from flask import Flask, jsonify
from scrap_service.listing_tracker_service_carlistmy.listing_tracker_carlistmy import ListingTrackerCarlistmy

app = Flask(__name__)
tracker = ListingTrackerCarlistmy()

@app.route('/track/listings/carlistmy', methods=['POST'])
def track_listings():
    """
    Endpoint untuk memeriksa status iklan di database.
    """
    tracker.track_listings()
    return jsonify({"message": "Proses tracking iklan dimulai"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5004, debug=True)
