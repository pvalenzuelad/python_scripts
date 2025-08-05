import csv
import os
import re
import time
import random
import requests
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
]

def get_output_dir():
    """Get output directory from temp.txt in script directory"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_file = os.path.join(script_dir, "temp.txt")
    output_dir = None
    try:
        with open(temp_file, "r") as f:
            for line in f:
                if line.startswith("output_dir="):
                    output_dir = line.strip().split("output_dir=")[1].strip()
                    print(f"📂 Output directory variable found: {output_dir}")
                    print(f"📍 Directory path: {output_dir}")
                    print(f"\nOutput directory: {output_dir}")  # Added explicit print
                    break
        if output_dir is None:
            print("⚠️ No 'output_dir' variable found in temp.txt")
        return output_dir
    except FileNotFoundError:
        print("⚠️ temp.txt not found in script directory")
        return None

def get_product_id(url):
    """Extract base product ID from URL"""
    match = re.search(r'-(\d+)-\d+\.html$', url)
    return match.group(1) if match else None

def sanitize_filename(url):
    """Create safe filename from URL"""
    return re.sub(r'[^a-zA-Z0-9-]', '_', url.split('/')[-1].split('.')[0])

def download_html_task(args, sleep_delay=True):
    url, referer, output_folder = args
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': referer,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive'
    }
    product_id = get_product_id(url)
    if product_id:
        filename = f"{product_id}.html"
    else:
        filename = sanitize_filename(url) + ".html"
    filepath = os.path.join(output_folder, filename)
    if os.path.exists(filepath):
        print(f"⚠️ ALERT: File already exists and will be skipped: {filename}")
        return (filename, filepath, url, True)
    try:
        if sleep_delay:
            time.sleep(random.uniform(1, 3))  # anti-bot delay
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"✅ Downloaded: {filename}")
            return (filename, filepath, url, False)
        else:
            print(f"⚠️ Failed to download: {url} (Status {response.status_code})")
            return (None, None, url, False)
    except Exception as e:
        print(f"🚨 Error downloading {url}: {e}")
        return (None, None, url, False)

def find_latest_csv():
    """Find the most recent obtained_urls_puma CSV file from output directory"""
    csv_files = []
    # Get output directory from temp.txt
    output_dir_candidate = get_output_dir()
    if output_dir_candidate is None:
        print("⚠️ Output directory not found!")
        sys.exit()
    if os.path.isabs(output_dir_candidate):
        output_dir = output_dir_candidate
    else:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), output_dir_candidate)
    if not output_dir or not os.path.isdir(output_dir):
        print("⚠️ Invalid output directory!")
        sys.exit()
        exit()

    # Look for CSV files in the output directory
    pattern = re.compile(r'^(\d{14})_obtained_urls_puma.*\.csv$')
    for filename in os.listdir(output_dir):
        match = pattern.match(filename)
        if match:
            try:
                timestamp = datetime.strptime(match.group(1), '%Y%m%d%H%M%S')
                csv_files.append((timestamp, os.path.join(output_dir, filename)))
            except ValueError:
                continue
        if not csv_files:
            print("⚠️ No matching CSV files found!")
            print("Files must be named: yyyymmddhhmmss_obtained_urls_puma[...].csv")
            sys.exit()
            exit()

    # Sort descending by timestamp
    csv_files.sort(reverse=True, key=lambda x: x[0])
    latest_file = csv_files[0][1]
    print(f"🔍 Found {len(csv_files)} matching CSV files")
    print(f"✅ Selected latest file: {latest_file}")
    return latest_file

def main():
    # Find latest CSV automatically
    input_csv = find_latest_csv()
    # Create output folder in the same directory as this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Use the folder where input_csv is located
    input_csv_dir = os.path.dirname(os.path.abspath(input_csv))
    output_folder = os.path.join(input_csv_dir, "htmls")
    os.makedirs(output_folder, exist_ok=True)
    # Output CSV path
    output_csv = os.path.join(output_folder, "complete_data.csv")

    # Gather download tasks and rows to write
    tasks = []
    rows_to_write = []
    task_row_indices = []
    with open(input_csv, 'r', encoding='utf-8') as f_in:
        reader = csv.DictReader(f_in, delimiter=';')
        fieldnames = list(reader.fieldnames)
        if 'html_filename' not in fieldnames:
            fieldnames.append('html_filename')
        if 'html_filepath' not in fieldnames:
            fieldnames.append('html_filepath')
        rows = list(reader)
        for row in rows:
            first_encounter_value = str(row.get('first_encounter', '')).strip().lower()
            if first_encounter_value == 'first encounter':
                crawled_url = row['crawled_url']
                referer_url = row['url']
                tasks.append((crawled_url, referer_url, output_folder))
                rows_to_write.append(row)
                rows_to_write.append(row)
    # Download in parallel with batching to avoid high memory usage
    max_workers = min(8, os.cpu_count() or 4)
    batch_size = 100  # Adjust batch size as needed for your memory constraints
    for batch_start in range(0, len(tasks), batch_size):
        batch_end = min(batch_start + batch_size, len(tasks))
        batch_tasks = tasks[batch_start:batch_end]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_rowidx = {
                executor.submit(download_html_task, batch_tasks[i], False): batch_start + i
                for i in range(len(batch_tasks))
            }
            for future in as_completed(future_to_rowidx):
                row_idx = future_to_rowidx[future]
                filename, filepath, _, _ = future.result()
                if filename and filepath:
                    rows_to_write[row_idx]['html_filename'] = filename
                    rows_to_write[row_idx]['html_filepath'] = filepath
                else:
                    rows_to_write[row_idx]['html_filename'] = ''
                    rows_to_write[row_idx]['html_filepath'] = ''
                rows_to_write[row_idx]['html_filepath'] = ''
    # Write output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for row in rows_to_write:
            writer.writerow(row)

    print(f"\nProcessing complete! Results saved to:\n{output_csv}")
    # Write output_dir_htmls to temp.txt
    relative_output_folder = os.path.relpath(output_folder, script_dir)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_file = os.path.join(script_dir, "temp.txt")
    with open(temp_file, "a") as f:
        f.write(f"\noutput_dir_htmls={relative_output_folder}")

    output_dir = get_output_dir()
    if output_dir:
        print(f"\n[INFO] Output directory from temp.txt: {output_dir}\n")
    else:
        print("\n[INFO] No output directory found in temp.txt\n")

if __name__ == "__main__":
    main()
