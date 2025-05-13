import argparse
from scrap_service.carlistmy_service_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari carlist.my")
    parser.add_argument("--brand", type=str, help="Nama brand (opsional)")
    parser.add_argument("--page", type=int, default=1, help="Halaman awal (default=1)")

    args = parser.parse_args()

    scraper = CarlistMyService()
    try:
        scraper.scrape_all_brands(start_brand=args.brand, start_page=args.page)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()