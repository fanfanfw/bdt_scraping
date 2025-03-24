from flask import Flask, jsonify
from scrap_service.imagedownload_service.imagedownload_service import ImageDownloadService

app = Flask(__name__)
image_service = ImageDownloadService()

@app.route('/download/images', methods=['POST'])
def download_images():
    """
    Endpoint untuk mendownload semua gambar dari database `cars`, berurutan dari ID terkecil ke terbesar.
    """
    image_service.run()
    return jsonify({"message": "Proses download semua gambar dimulai"}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5003, debug=True)
