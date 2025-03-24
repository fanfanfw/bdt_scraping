from flask import Flask, jsonify, request
from scrap_service.listing_tracker_service_mudahmy.listing_tracker_mudahmy import ListingTrackerMudahmy

app = Flask(__name__)
tracker = ListingTrackerMudahmy()

@app.route('/track/listings/mudahmy', methods=['POST'])
def track_listings():
    """
    Endpoint untuk memeriksa status iklan di database berdasarkan ID.
    """
    data = request.get_json()
    start_id = data.get("id", 1) 
    tracker.track_listings(start_id=start_id)
    return jsonify({"message": f"Proses tracking iklan dimulai dari ID {start_id}"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5005, debug=True)