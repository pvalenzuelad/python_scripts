import os
import csv
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(script_dir, 'temp.txt'), 'r') as f:
    for line in f:
        if line.startswith('output_dir='):
            input_folder = line.split('=')[1].strip()
            break
input_folder = os.path.join(script_dir, input_folder)
output_csv = os.path.join(input_folder, "obtained_data_htmls_puma.csv")

def get_output_dir():
    """Get output directory and output_dir_htmls from temp.txt in script directory"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_file = os.path.join(script_dir, "temp.txt")
    output_dir = None
    output_dir_htmls = None
    try:
        with open(temp_file, "r") as f:
            for line in f:
                if line.startswith("output_dir="):
                    output_dir = line.strip().split("output_dir=")[1].strip()
                    print(f"📂 Output directory variable found: {output_dir}")
                    print(f"📍 Directory path: {output_dir}")
                    print(f"\nOutput directory: {output_dir}")
                elif line.startswith("output_dir_htmls="):
                    output_dir_htmls = line.strip().split("output_dir_htmls=")[1].strip()
                    print(f"📂 Output directory htmls found: {output_dir_htmls}")
        if output_dir is None:
            print("⚠️ No 'output_dir' variable found in temp.txt")
        if output_dir_htmls is None:
            print("⚠️ No 'output_dir_htmls' variable found in temp.txt")
        return output_dir_htmls if output_dir_htmls else output_dir
    except FileNotFoundError:
        print("⚠️ temp.txt not found in script directory")
        return None

def extract_data_from_html(html_path):
    filename = os.path.basename(html_path)
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        scripts = soup.find_all('script', {'type': 'text/x-magento-init'})

        # Product name
        product_name = soup.title.string.strip() if soup.title else "Unknown Product"

        # Magento data extraction
        product_options_data = None
        for script in scripts:
            if script.string and '#product_addtocart_form' in script.string:
                try:
                    product_options_data = json.loads(script.string)
                    break
                except Exception:
                    continue

        if not product_options_data:
            return []

        try:
            spConfig = product_options_data['#product_addtocart_form']['configurable']['spConfig']
        except Exception:
            return []

        # Attribute identification
        color_attr = next((a for a in spConfig['attributes'].values()
                          if a['code'] in ['color', 'colour', 'tinte']), None)
        size_attr = next((a for a in spConfig['attributes'].values()
                         if a['code'] in ['size', 'talla']), None)
        if not color_attr or not size_attr:
            return []

        # Color-size mapping
        color_options = {
            opt['label']: set(str(p) for p in opt['products'])
            for opt in color_attr['options']
        }

        # Price and stock data
        option_prices = spConfig.get('optionPrices', {})
        currency = spConfig.get('currencySymbol', '$')
        execution_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for color_label, color_skus in color_options.items():
            for size_option in size_attr['options']:
                size_sku_set = set(str(p) for p in size_option['all_products'])
                common_skus = color_skus & size_sku_set
                if not common_skus:
                    continue
                sku = common_skus.pop()
                availability = "Out of Stock" if sku in size_option.get('out_of_stock', []) else "In Stock"
                price_data = option_prices.get(sku, {})
                original_price = price_data.get('oldPrice')
                if isinstance(original_price, dict):
                    original_price = original_price.get('amount', 'N/A')
                if original_price is None:
                    original_price = spConfig.get('basePrice', {}).get('amount', 'N/A')
                discounted_price = price_data.get('finalPrice')
                if isinstance(discounted_price, dict):
                    discounted_price = discounted_price.get('amount', 'N/A')
                if discounted_price is None:
                    discounted_price = original_price
                rows.append([
                    filename,
                    color_label,
                    size_option['label'],
                    sku,
                    availability,
                    f"{currency}{original_price}",
                    f"{currency}{discounted_price}",
                    product_name,
                    execution_datetime
                ])
        print(f"✅ Processed: {filename}")
        return rows
    except Exception as e:
        print(f"🚨 Error processing {filename}: {str(e)}")
        return []

header = [
    "html_filename", "color", "size", "sku", "availability",
    "original_price", "discounted_price", "product_name", "extraction_datetime"
]

with open(output_csv, 'w', newline='', encoding='utf-8') as f_out:
    writer = csv.writer(f_out, delimiter=';')
    writer.writerow(header)
    output_dir_htmls = get_output_dir()
    html_files = [
        os.path.join(script_dir, output_dir_htmls, f)
        for f in os.listdir(os.path.join(script_dir, output_dir_htmls))
        if f.endswith('.html')
    ]
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        futures = {executor.submit(extract_data_from_html, file): file for file in html_files}
        for future in as_completed(futures):
            for row in future.result():
                writer.writerow(row)

print(f"✅ Extraction complete! Data saved to {output_csv}")
