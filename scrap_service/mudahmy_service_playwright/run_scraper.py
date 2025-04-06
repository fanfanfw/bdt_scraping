import argparse
from scrap_service.mudahmy_service_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari mudah.my")
    parser.add_argument("--brand", type=str, help="Nama brand (opsional)")
    parser.add_argument("--model", type=str, help="Nama model (opsional)")
    parser.add_argument("--page", type=int, default=1, help="Halaman awal (default=1)")

    args = parser.parse_args()

    scraper = MudahMyService()
    try:
        scraper.scrape_all_brands(brand=args.brand, model=args.model, start_page=args.page)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
