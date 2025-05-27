import csv
import time
import random
import logging
import sys
from pathlib import Path
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("get_scraping_postalcode.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)

def log_info(msg):
    logging.info(msg)
    for handler in logging.getLogger().handlers:
        handler.flush()

def log_warning(msg):
    logging.warning(msg)
    for handler in logging.getLogger().handlers:
        handler.flush()

def log_error(msg):
    logging.error(msg)
    for handler in logging.getLogger().handlers:
        handler.flush()

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

INPUT_CSV = "scraped_links_location_part1.csv"  
OUTPUT_CSV = "scraped_details_part1.csv"

def parse_proxy_pool(proxy_str):
    proxies = []
    for p in proxy_str.split(","):
        parts = p.strip().split(":")
        if len(parts) == 4:
            ip, port, user, pwd = parts
            proxies.append(
                {"server": f"http://{ip}:{port}", "username": user, "password": pwd}
            )
    return proxies

def write_to_csv(filepath, rows, write_header=False):
    mode = "a" if Path(filepath).exists() else "w"
    with open(filepath, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header and mode == "w":
            writer.writerow(
                [
                    "url",
                    "scrap_url",
                    "location",
                    "post_office",
                    "state",
                    "post_code",
                    "latitude",
                    "longitude",
                ]
            )
        for row in rows:
            writer.writerow(row)

def click_consent_if_present(page):
    try:
        consent_selector = "button:has-text('Consent')"
        if page.query_selector(consent_selector):
            page.click(consent_selector)
            log_info("Popup consent ditemukan dan tombol Consent diklik.")
            time.sleep(2)
    except PlaywrightTimeoutError:
        pass
    except Exception as e:
        log_warning(f"Error saat klik consent: {e}")

def scrape_detail(page, url):
    try:
        page.goto(url, timeout=60000)
        page.wait_for_selector("#t2", timeout=15000)
        click_consent_if_present(page)

        tables = page.query_selector_all("#t2")
        if len(tables) < 2:
            latitude = ""
            longitude = ""
        else:
            lat_table = tables[1]
            latitude_el = lat_table.query_selector("tbody > tr:nth-child(2) > td:nth-child(2)")
            latitude = latitude_el.inner_text().strip() if latitude_el else ""
            longitude_el = lat_table.query_selector("tbody > tr:nth-child(3) > td:nth-child(2)")
            longitude = longitude_el.inner_text().strip() if longitude_el else ""

        loc_table = tables[0]
        location = loc_table.query_selector("tbody > tr:nth-child(2) > td:nth-child(2)").inner_text().strip()
        post_office = loc_table.query_selector("tbody > tr:nth-child(3) > td:nth-child(2)").inner_text().strip()
        state = loc_table.query_selector("tbody > tr:nth-child(4) > td:nth-child(2)").inner_text().strip()
        post_code = loc_table.query_selector("tbody > tr:nth-child(5) > td:nth-child(2)").inner_text().strip()

        return location, post_office, state, post_code, latitude, longitude

    except Exception as e:
        raise e

def check_proxy_ip(page):
    try:
        page.goto("https://ip.oxylabs.io/", timeout=15000)
        ip = page.inner_text("body").strip()
        log_info(f"cek ip : {ip}")
        return ip
    except Exception as e:
        log_warning(f"Error cek IP proxy: {e}")
        return None

def main():
    proxies = parse_proxy_pool(PROXY_POOL_STR)

    urls_to_scrape = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "scrap_url" in row and row["scrap_url"]:
                urls_to_scrape.append((row["url"], row["scrap_url"]))

    log_info(f"Total scrap_url to process: {len(urls_to_scrape)}")

    if not Path(OUTPUT_CSV).exists():
        write_to_csv(OUTPUT_CSV, [], write_header=True)

    with sync_playwright() as p:
        browser_type = p.chromium

        browser = None
        page = None
        proxy = None

        def init_browser_with_proxy():
            nonlocal browser, page, proxy
            if browser:
                browser.close()
            proxy = random.choice(proxies) if proxies else None
            log_info(f"browser init dengan proxy {proxy['server'] if proxy else 'NO PROXY'}")
            launch_args = {}
            if proxy:
                launch_args["proxy"] = proxy
            browser = browser_type.launch(headless=False, **launch_args)
            page = browser.new_page()
            check_proxy_ip(page)
            return browser, page

        browser, page = init_browser_with_proxy()

        count = 0
        max_retries = 3
        i = 0

        while i < len(urls_to_scrape):
            base_url, scrap_url = urls_to_scrape[i]
            full_url = scrap_url if scrap_url.startswith("http") else "https://postcode.my" + scrap_url

            log_info(f"scrap_url {count + 1}: {full_url}")

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    location, post_office, state, post_code, latitude, longitude = scrape_detail(page, full_url)

                    if location:
                        write_to_csv(
                            OUTPUT_CSV,
                            [
                                [
                                    base_url,
                                    full_url,
                                    location,
                                    post_office,
                                    state,
                                    post_code,
                                    latitude,
                                    longitude,
                                ]
                            ],
                        )
                        log_info("berhasil disimpan")
                    else:
                        log_warning(f"Data tidak ditemukan di {full_url}")

                    success = True
                    count += 1

                    if count % 10 == 0:
                        log_info("tutup browser")
                        browser.close()
                        browser, page = init_browser_with_proxy()

                    delay = random.uniform(15, 30)
                    log_info(f"tunggu : {delay:.1f} detik")
                    time.sleep(delay)

                except PlaywrightTimeoutError as e:
                    log_warning(f"Timeout error scraping detail {full_url}: {e}")
                    retry_count += 1
                    log_info(f"Reinit browser dan retry ({retry_count}/{max_retries})...")
                    browser.close()
                    browser, page = init_browser_with_proxy()
                except PlaywrightError as e:
                    log_warning(f"Playwright error scraping detail {full_url}: {e}")
                    retry_count += 1
                    log_info(f"Reinit browser dan retry ({retry_count}/{max_retries})...")
                    browser.close()
                    browser, page = init_browser_with_proxy()
                except Exception as e:
                    log_warning(f"Error scraping detail {full_url}: {e}")
                    retry_count += 1
                    log_info(f"Reinit browser dan retry ({retry_count}/{max_retries})...")
                    browser.close()
                    browser, page = init_browser_with_proxy()

            if not success:
                log_error(f"Gagal scrap {full_url} setelah {max_retries} percobaan.")

            i += 1

        if browser:
            browser.close()

    log_info("Scraping selesai.")

if __name__ == "__main__":
    main()