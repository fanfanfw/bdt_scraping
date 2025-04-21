import sys
import argparse
from carlistmy_service import CarlistMyService


def parse_arguments():
    parser = argparse.ArgumentParser(description="Scraping CarlistMY berdasarkan brand dan halaman")

    # Menambahkan parameter brand dan page
    parser.add_argument('--brand', type=str, help='Nama brand mobil untuk scraping', required=True)
    parser.add_argument('--page', type=int, help='Nomor halaman untuk scraping', default=1)

    return parser.parse_args()


def main():
    # Parsing argument dari CLI
    args = parse_arguments()

    # Inisialisasi service scraping
    carlistmy_scraper = CarlistMyService()

    # Menjalankan scraping dengan brand dan page yang diterima dari CLI
    try:
        carlistmy_scraper.scrape_all_brands(start_brand=args.brand, start_page=args.page)
    except KeyboardInterrupt:
        print("\nScraping dihentikan.")
        carlistmy_scraper.stop_scraping()
        sys.exit(0)


if __name__ == "__main__":
    main()
