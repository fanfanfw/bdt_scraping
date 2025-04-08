import argparse
from scrap_service.listing_tracker_service_carlistmy_playwright.listing_tracker_carlistmy_playwright import ListingTrackerCarlistmyPlaywright
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Listing Tracker Carlist.my")
    parser.add_argument("--start-id", type=int, default=1, help="Mulai dari ID keberapa (default: 1)")
    args = parser.parse_args()

    tracker = ListingTrackerCarlistmyPlaywright()
    tracker.track_listings(start_id=args.start_id)

if __name__ == "__main__":
    main()
