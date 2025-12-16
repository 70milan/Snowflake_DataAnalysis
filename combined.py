#!/usr/bin/env python3
"""
combined.py

Downloads the Data.gov dataset (JSON by default) and uploads its rows into
Snowflake using batched INSERT ... PARSE_JSON. Shows a progress bar for both
download and upload (accurate for upload because we control the row inserts).

Usage:
  python combined.py [DATASET_URL] --out downloads --batch 500

Requires: requests, beautifulsoup4, tqdm, python-dotenv, snowflake-connector-python
Set Snowflake credentials in environment or a .env file: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import os, sys, argparse, re, json
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
    import snowflake.connector
except ModuleNotFoundError as e:
    sys.exit(f"Missing package: {e.name}. Install with: pip install requests beautifulsoup4 tqdm snowflake-connector-python")

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Configuration from env
SF_USER = os.getenv('SNOWFLAKE_USER')
SF_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SF_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SF_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SF_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SF_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SF_TABLE = os.getenv('TABLE_NAME', 'BRONZE_BORDER')

REQUIRED = [('SNOWFLAKE_USER', SF_USER), ('SNOWFLAKE_PASSWORD', SF_PASSWORD), ('SNOWFLAKE_ACCOUNT', SF_ACCOUNT), ('SNOWFLAKE_DATABASE', SF_DATABASE), ('SNOWFLAKE_SCHEMA', SF_SCHEMA)]
missing = [n for n,v in REQUIRED if not v]
if missing:
    sys.exit(f"Missing env vars: {', '.join(missing)}. Add them or create a .env file.")


def get_page(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def find_json_resources(page_url, html):
    # Try CKAN
    p = urlparse(page_url)
    out = []
    if 'catalog.data.gov' in p.netloc.lower():
        try:
            slug = p.path.strip('/').split('/')[-1]
            api = f'https://catalog.data.gov/api/3/action/package_show?id={slug}'
            r = session.get(api, timeout=10); r.raise_for_status(); data = r.json()
            for res in data.get('result', {}).get('resources', []):
                url = res.get('url') or res.get('download_url')
                if not url: continue
                fmt = (res.get('format') or '').lower()
                if 'json' in fmt or url.lower().endswith('.json') or 'rows.json' in url.lower():
                    out.append(url)
            if out: return out
        except Exception:
            pass
    # Fallback: scrape
    soup = BeautifulSoup(html, 'html.parser')
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        full = urljoin(page_url, href)
        if re.search(r"\.json($|\?)", full, re.I) or 'rows.json' in full.lower():
            out.append(full)
    return out


def download_with_progress(session, url, out_dir, filename=None):
    with session.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        if filename:
            name = filename
        else:
            name = os.path.basename(urlparse(r.url).path) or 'data.json'
        name = re.sub(r"[\\/:*?\"<>|]", '_', name)
        out_path = os.path.join(out_dir, name)
        total = r.headers.get('content-length')
        total = int(total) if total and total.isdigit() else None
        if total:
            with open(out_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc=name) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
        else:
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return out_path


def parse_json_rows(path):
    rows = []
    with open(path, 'r', encoding='utf-8') as fh:
        text = fh.read().strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                rows = parsed
            elif isinstance(parsed, dict):
                rows = [parsed]
        except json.JSONDecodeError:
            # try NDJSON
            fh.seek(0)
            for line in fh:
                line = line.strip()
                if not line: continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    return rows


def upload_rows_to_snowflake(rows, batch_size=500):
    conn = snowflake.connector.connect(user=SF_USER, password=SF_PASSWORD, account=SF_ACCOUNT, warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA)
    cur = conn.cursor()
    try:
        full_table = f"{SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}"
        insert_sql = f"INSERT INTO {full_table} (data) VALUES (PARSE_JSON(%s))"
        total = len(rows)
        with tqdm(total=total, desc='Upload rows', unit='rows') as bar:
            for i in range(0, total, batch_size):
                batch = rows[i:i+batch_size]
                params = [(json.dumps(r),) for r in batch]
                cur.executemany(insert_sql, params)
                conn.commit()
                bar.update(len(batch))
    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('url', nargs='?', default='https://catalog.data.gov/dataset/border-crossing-entry-data-683ae')
    p.add_argument('--out', '-o', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads'))
    p.add_argument('--batch', type=int, default=500)
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    session = requests.Session()
    session.headers.update({'User-Agent': 'combined-downloader/1.0'})

    print('Fetching dataset page...')
    try:
        html = get_page(session, args.url)
    except Exception as e:
        sys.exit(f'Failed to fetch page: {e}')

    resources = find_json_resources(args.url, html)
    if not resources:
        sys.exit('No JSON resources found on the page')

    # choose first JSON resource (rows.json preferred)
    target = None
    for r in resources:
        if 'rows.json' in r.lower():
            target = r; break
    if not target:
        target = resources[0]

    print('Downloading:', target)
    local = download_with_progress(session, target, args.out, filename='border_crossing_dataset.json')
    print('Downloaded to', local)

    print('Parsing JSON rows...')
    rows = parse_json_rows(local)
    print(f'Parsed {len(rows)} rows')

    if not rows:
        sys.exit('No rows to upload')

    print('Uploading rows to Snowflake...')
    upload_rows_to_snowflake(rows, batch_size=args.batch)
    print('Upload complete')
