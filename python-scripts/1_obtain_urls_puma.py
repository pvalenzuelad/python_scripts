import re
import requests
import time
import random
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from datetime import datetime
from pathlib import Path

# Get absolute path to the directory containing this script
SCRIPT_DIR = Path(__file__).parent.resolve()

# Get timestamp at script start
start_time = datetime.now()
timestamp_str = start_time.strftime("%Y%m%d%H%M%S")
output_filename = f"{timestamp_str}_obtained_urls_puma.csv"

# Create downloaded_htmls/<timestamp_str> directory for saving HTMLs
downloaded_htmls_dir = SCRIPT_DIR / 'downloaded_htmls'
session_html_dir = downloaded_htmls_dir / timestamp_str
session_html_dir.mkdir(parents=True, exist_ok=True)
output_filepath = session_html_dir / output_filename

# Create a temp file named temp.txt
temp_file_path = SCRIPT_DIR / 'temp.txt'
with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
    temp_file.write('Temporary file for process status.\n')
    temp_file.write(f'output_dir={session_html_dir.relative_to(SCRIPT_DIR)}\n')
    print(f"\n[SESSION]Session HTML directory: {session_html_dir}")

def save_html_content(url, html, session_html_dir=session_html_dir):
    # Create a safe filename from the URL
    parsed = urlparse(url)
    safe_path = parsed.path.strip('/').replace('/', '_')
    if not safe_path:
        safe_path = 'index'
    filename = f"{parsed.netloc}_{safe_path}.html"
    filepath = session_html_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]

def get_html(url):
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        time.sleep(random.uniform(1, 2))
        response = requests.get(url, headers=headers, timeout=15)
        return response.text if response.status_code == 200 else None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def is_internal_puma_url(url):
    parsed = urlparse(url)
    return parsed.netloc == 'cl.puma.com'

def normalize_url(url):
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))

def get_page_links(url):
    html = get_html(url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    links = []
    for a in soup.find_all('a', href=True):
        abs_url = urljoin(url, a['href'])
        abs_url = normalize_url(abs_url)
        if is_internal_puma_url(abs_url):
            links.append(abs_url)
    return list(set(links))

def load_blacklist():
    blacklist = set()
    blacklist_path = SCRIPT_DIR / 'blacklist_url_puma.csv'
    try:
        with open(blacklist_path, 'r', encoding='utf-8') as bl_file:
            for line in bl_file:
                url = line.strip()
                if url:
                    normalized = normalize_url(url)
                    blacklist.add(normalized)
        print(f"[INFO] Loaded {len(blacklist)} blacklisted URLs.")
    except FileNotFoundError:
        print("[WARNING] Blacklist file not found - proceeding without filtering.")
    except Exception as e:
        print(f"[ERROR] Error loading blacklist: {e}")
    return blacklist

def is_product_url(url):
    # Match URLs ending with -<digits>-<digits>.html (e.g., ...-397647-03.html)
    return bool(re.search(r'-\d+-\d+\.html$', url))

def process_urls(input_file, output_file):
    blacklist = load_blacklist()
    seen_product_ids = set()  # Track across all input URLs
    input_path = SCRIPT_DIR / input_file
    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, skipinitialspace=True)
        original_headers = [h.strip() for h in reader.fieldnames]
        rows = list(reader)
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        new_headers = original_headers + ['crawled_url', 'category', 'first_encounter', 'datetime']
        # NOTE: The CSV delimiter is set to ';' (semicolon) intentionally. 
        # Some tools may expect ',' (comma) as the default delimiter. Adjust as needed.
        writer = csv.DictWriter(outfile, fieldnames=new_headers, delimiter=';')
        writer.writeheader()
        for row in rows:
            cleaned_row = {k.strip(): v for k, v in row.items()}
            original_url = cleaned_row.get('url', '')
            if not original_url:
                continue
            base_url = original_url.split('?')[0]
            unique_links = set()  # Deduplicate per input URL
            all_crawled_urls = []
            print(f"\n[PROCESS] Starting URL: {original_url}")
            for page in range(1, 99):
                current_url = f"{base_url}?p={page}" if page > 1 else base_url
                print(f"[PAGE] Processing: {current_url}")
                crawled_urls = get_page_links(current_url)
                print(f"[INFO] Found {len(crawled_urls)} links on this page before filtering.")
                filtered_urls = []
                for url in crawled_urls:
                    if url in blacklist:
                        print(f"[BLACKLISTED] Skipping blacklisted URL: {url}")
                    else:
                        filtered_urls.append(url)
                print(f"[INFO] {len(filtered_urls)} links remain after blacklist filtering.")
                new_links = [url for url in filtered_urls if url not in unique_links]
                print(f"[INFO] {len(new_links)} new links found on this page.")
                if not new_links and page > 1:
                    print(f"[STOP] No new links found on page {page}, stopping pagination for this URL.")
                    break
                unique_links.update(new_links)  # Only add truly new links
                all_crawled_urls.extend(new_links)  # Only keep truly new links
            for crawled_url in all_crawled_urls:
                category = 'product' if is_product_url(crawled_url) else 'not product'
                first_encounter = ''
                if category == 'product':
                    match = re.search(r'-(\d+)-\d+\.html$', crawled_url)
                    if not match:
                        # Try to match any digits before .html as fallback
                        match = re.search(r'(\d+)\.html$', crawled_url)
                    if match:
                        product_id = match.group(1)
                        if product_id not in seen_product_ids:
                            first_encounter = 'first encounter'
                            seen_product_ids.add(product_id)
                new_row = cleaned_row.copy()
                new_row.update({
                    'crawled_url': crawled_url,
                    'category': category,
                    'first_encounter': first_encounter,
                    'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                writer.writerow(new_row)
            print(f"[DONE] Finished processing: {original_url}")

if __name__ == "__main__":
    process_urls("input_urls_puma.csv", output_filepath)
    print(f"\n[COMPLETE] Crawling completed. Results saved to {output_filepath}")
    # If the process finished correctly, set output_dir variable
    output_dir = str(session_html_dir)
    # Get only the subdirectory part relative to SCRIPT_DIR
    try:
        relative_output_dir = Path(output_dir).relative_to(SCRIPT_DIR)
        relative_path_str = str(relative_output_dir)
    except ValueError:
        # Fallback to absolute path if not a subpath
        relative_path_str = str(output_dir)

    relative_path_str = str(relative_output_dir)
