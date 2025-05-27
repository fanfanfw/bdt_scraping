import csv
import time
import random
import logging
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

INPUT_CSV = "postalcode.csv"
OUTPUT_CSV = "scraped_links_location.csv"

PROXY_POOL_STR = (
    "45.38.111.15:5930:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.218.92:6444:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.243.224:7545:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.223.218:6060:FOS13Proxy8208:FOS13Proxy8208,"
    "104.252.149.78:5492:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.249.116:7453:FOS13Proxy8208:FOS13Proxy8208,"
    "45.252.59.243:6269:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.231.211:7525:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.243.186:7507:FOS13Proxy8208:FOS13Proxy8208,"
    "104.252.149.236:5650:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.243.204:7525:FOS13Proxy8208:FOS13Proxy8208,"
    "94.176.106.245:6659:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.231.18:7332:FOS13Proxy8208:FOS13Proxy8208,"
    "45.252.59.58:6084:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.244.34:5357:FOS13Proxy8208:FOS13Proxy8208,"
    "45.252.59.70:6096:FOS13Proxy8208:FOS13Proxy8208,"
    "45.39.73.162:5577:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.227.55:7887:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.218.57:6409:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.244.47:5370:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.218.60:6412:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.219.2:6343:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.245.245:6569:FOS13Proxy8208:FOS13Proxy8208,"
    "45.249.104.121:6416:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.244.225:5548:FOS13Proxy8208:FOS13Proxy8208,"
    "45.249.104.172:6467:FOS13Proxy8208:FOS13Proxy8208,"
    "104.252.140.81:5997:FOS13Proxy8208:FOS13Proxy8208,"
    "104.252.149.164:5578:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.219.216:6557:FOS13Proxy8208:FOS13Proxy8208,"
    "82.21.232.174:8013:FOS13Proxy8208:FOS13Proxy8208"
)

log_file = "get_scraping_url.log"

file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(file_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler],
)

def log_info(msg):
    logging.info(msg)
    for h in logging.getLogger().handlers:
        h.flush()

def parse_proxies(proxy_str):
    proxies = []
    for entry in proxy_str.split(","):
        parts = entry.strip().split(":")
        if len(parts) == 4:
            ip, port, user, pw = parts
            proxies.append({
                "server": f"http://{ip}:{port}",
                "username": user,
                "password": pw
            })
    return proxies

def read_urls_from_csv(filepath):
    with open(filepath, newline='', encoding='utf-8') as f:
        return [row['url'] for row in csv.DictReader(f) if row['url']]

def write_links_to_csv(filepath, rows, write_header=False):
    mode = 'a' if Path(filepath).exists() else 'w'
    with open(filepath, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header and mode == 'w':
            writer.writerow(["url", "scrap_url"])
        writer.writerows(rows)

def check_proxy_ip(browser_type, proxy):
    try:
        browser = browser_type.launch(headless=False, proxy=proxy)
        page = browser.new_page()
        page.goto("https://ip.oxylabs.io/", timeout=15000)
        ip = page.inner_text("body").strip()
        log_info(f"✅ Proxy OK, IP: {ip}")
        browser.close()
        return True
    except Exception as e:
        log_info(f"❌ Proxy check failed: {proxy['server']} – {e}")
        return False

def handle_consent_popup(page):
    try:
        consent_btn = page.wait_for_selector("button:has-text('Consent')", timeout=5000)
        if consent_btn:
            consent_btn.click()
            log_info("✅ Consent popup diklik")
            time.sleep(2)
    except Exception:
        pass

def scrape_links_from_page(page):
    links = []
    for row in page.query_selector_all("#t2 tbody tr:not(:first-child)"):
        a = row.query_selector("td:nth-child(1) a")
        if a:
            href = a.get_attribute("href")
            if href and href.strip() and href != "#":
                links.append(href)
    return links

def get_next_page_url(page, retries=3, delay=3):
    for attempt in range(retries):
        try:
            anchors = page.query_selector_all("ul.pagination li a")
            for a in anchors:
                text = a.inner_text().strip()
                if text in [">", "›", "Next", "»"]:
                    href = a.get_attribute("href")
                    if href and href.strip():
                        log_info(f"➡️  Next page detected (retry {attempt+1})")
                        return href
        except Exception as e:
            log_info(f"Retry {attempt+1}: Error detecting pagination: {e}")
        time.sleep(delay)
    log_info("⚠️  No next page found after retries.")
    return None

def main():
    urls = read_urls_from_csv(INPUT_CSV)
    proxies = parse_proxies(PROXY_POOL_STR)
    log_info(f"🔍 Total URL: {len(urls)}, Total Proxy: {len(proxies)}")

    if not Path(OUTPUT_CSV).exists():
        write_links_to_csv(OUTPUT_CSV, [], write_header=True)

    with sync_playwright() as p:
        browser_type = p.chromium
        base_url = "https://postcode.my"

        for base_url_with_page in urls:
            proxy = None
            candidates = proxies.copy()
            while candidates:
                proxy_try = random.choice(candidates)
                log_info(f"🌐 Trying proxy: {proxy_try['server']}")
                if check_proxy_ip(browser_type, proxy_try):
                    proxy = proxy_try
                    break
                else:
                    candidates.remove(proxy_try)
            if proxy is None:
                log_info("🚨 No valid proxy found. Skipping URL.")
                continue

            browser = browser_type.launch(headless=False, proxy=proxy)
            page = browser.new_page()
            current_url = base_url_with_page

            while True:
                try:
                    page.goto(current_url, timeout=60000)
                    time.sleep(5)
                    handle_consent_popup(page)
                    page.wait_for_selector("#t2", timeout=30000)

                    links = scrape_links_from_page(page)
                    full_links = [link if link.startswith("http") else base_url + link for link in links]

                    write_links_to_csv(OUTPUT_CSV, [(current_url, link) for link in full_links])
                    log_info(f"✅ {len(full_links)} links scraped from {current_url}")

                    time.sleep(random.uniform(15, 30))

                    next_href = get_next_page_url(page)
                    if next_href:
                        current_url = base_url + next_href
                        log_info(f"➡️  Next: {current_url}")
                    else:
                        log_info(f"✅ Done scraping all pages for {base_url_with_page}")
                        break
                except Exception as e:
                    log_info(f"❌ Error scraping {current_url}: {e}")
                    break

            browser.close()

    log_info(f"🎉 Scraping complete. Data saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()