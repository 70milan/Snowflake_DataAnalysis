#!/usr/bin/env python3
"""Compact Data.gov downloader (JSON preferred)."""

import argparse, os, re, sys
from urllib.parse import urlparse, unquote
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_filename(response, url):
    """Extracts filename from Content-Disposition header or URL."""
    cd = response.headers.get("content-disposition")
    if cd:
        fname = re.findall('filename\*?=([^;]+)', cd, flags=re.IGNORECASE)
        if fname:
            clean_name = fname[0].strip().strip('"').strip("'")
            if "UTF-8''" in clean_name:
                clean_name = unquote(clean_name.split("UTF-8''")[-1])
            return clean_name
    return os.path.basename(urlparse(url).path) or "downloaded_data.json"

def get_ckan_resources(session, url):
    """Attempts to fetch resources via CKAN API."""
    if "catalog.data.gov" not in url: return []
    try:
        dataset_id = urlparse(url).path.strip('/').split('/')[-1]
        api = f"https://catalog.data.gov/api/3/action/package_show?id={dataset_id}"
        res = session.get(api, timeout=10).json()
        return [
            {'url': r.get('url'), 'fmt': r.get('format', '').lower()} 
            for r in res.get('result', {}).get('resources', [])
        ]
    except Exception:
        return []

def scrape_fallback(url, html):
    """Fallback: Scrapes simple hrefs ending in data extensions."""
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a in soup.find_all('a', href=True):
        full = a['href']
        if re.search(r"\.(json|csv|zip|xlsx?)$", full, re.I):
            links.add(full)
    return [{'url': l, 'fmt': 'unknown'} for l in links]

def download(session, url, out_dir, force_filename=None):
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            
            # Use forced filename if provided, otherwise detect from headers/url
            if force_filename:
                name = force_filename
            else:
                name = re.sub(r"[\\/:*?\"<>|]", '_', get_filename(r, url))
            
            dest = os.path.join(out_dir, name)
            
            total = int(r.headers.get('content-length', 0))
            with open(dest, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc=name) as bar:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    bar.update(len(chunk))
            return dest
    except Exception as e:
        print(f"Failed {url}: {e}", file=sys.stderr)
        return None

def main():
    p = argparse.ArgumentParser()
    p.add_argument('url', nargs='?', default='https://catalog.data.gov/dataset/border-crossing-entry-data-683ae')
    p.add_argument('--out', '-o', default='downloads')
    p.add_argument('--all', dest='only_json', action='store_false', help="Download all formats, not just JSON")
    p.set_defaults(only_json=True)
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "downloader/2.0"})

    print(f"Fetching metadata for: {args.url}")
    try:
        resources = get_ckan_resources(session, args.url)
        if not resources:
            html = session.get(args.url).text
            resources = scrape_fallback(args.url, html)
    except Exception as e:
        sys.exit(f"Error fetching page: {e}")

    # Filter for JSON if requested
    if args.only_json:
        resources = [r for r in resources if 'json' in r['fmt'] or r['url'].lower().endswith('.json')]

    if not resources:
        sys.exit("No matching resources found.")

    print(f"Found {len(resources)} file(s). Downloading...")
    for res in resources:
        # Force the filename for JSON resources
        target_name = "border_crossing_dataset.json" if args.only_json else None
        
        path = download(session, res['url'], args.out, force_filename=target_name)
        if path:
            print(f" -> Saved to: {path}")



if __name__ == '__main__':
    main()