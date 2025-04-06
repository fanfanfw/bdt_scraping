from scrap_service.mudahmy_service_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    scraper = MudahMyService()
    try:
        scraper.sync_to_cars()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
