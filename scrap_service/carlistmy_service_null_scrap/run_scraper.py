from scrap_service.carlistmy_service_null_scrap.carlist_null_service import CarlistMyNullService

import sys

if __name__ == "__main__":
    scraper = CarlistMyNullService()
    try:
        scraper.scrape_null_entries()
    except KeyboardInterrupt:
        print("\n🛑 Scraping dihentikan oleh user.")
        scraper.stop_scraping()
        sys.exit(0)
