from flask import Flask, jsonify, request
from scrap_service.listing_tracker_service_carlistmy.listing_tracker_carlistmy import ListingTrackerCarlistmy

app = Flask(__name__)
tracker = ListingTrackerCarlistmy()

@app.route('/track/listings/carlistmy', methods=['POST'])
def track_listings():
    """
    Endpoint untuk memeriksa status iklan di database.
    Menerima JSON dengan key "id" untuk memulai pengecekan dari ID tertentu.
    """
    data = request.get_json()
    start_id = data.get("id", 1) 
    tracker.track_listings(start_id=start_id)
    return jsonify({"message": f"Proses tracking iklan dimulai dari ID {start_id}"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5004, debug=True)