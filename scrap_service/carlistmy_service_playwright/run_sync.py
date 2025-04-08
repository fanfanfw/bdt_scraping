from scrap_service.carlistmy_service_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    scraper = CarlistMyService()
    try:
        scraper.sync_to_cars()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
