import argparse
from dotenv import load_dotenv
from scrap_service.listing_tracker_service_carlistmy_playwright.listing_tracker_carlistmy_playwright import ListingTrackerCarlistmyPlaywright

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Listing Tracker Carlist.my")
    parser.add_argument("--start-id", type=int, default=1, help="Mulai dari ID keberapa (default: 1)")
    parser.add_argument(
        "--status",
        type=str,
        choices=["unknown", "active", "all"],
        default="all",
        help="Status listing yang ingin dicek: unknown, active, atau all (default: all)"
    )

    args = parser.parse_args()

    tracker = ListingTrackerCarlistmyPlaywright()
    tracker.track_listings(start_id=args.start_id, status_filter=args.status)

if __name__ == "__main__":
    main()
