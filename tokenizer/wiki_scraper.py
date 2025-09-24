#!/usr/bin/env python3
"""
Wikipedia category scraper (MediaWiki API)
Collects plaintext article extracts from specified categories and languages.

Outputs: JSONL file with objects:
{ "lang": "en", "category_path": ["Physics","Astrophysics"], "title": "...", "pageid": 123, "text": "...", "url": "https://en.wikipedia.org/wiki/..." }

Usage example:
python scraper_wikipedia_categories.py --langs en sr zh --categories "Physics" "Mathematics" "Computer_science" --max-pages 100 --depth 2
"""

import requests
import time
import json
from urllib.parse import quote

# ------------ Config / politeness ------------
USER_AGENT = "handwriting-tokenizer-scraper/1.0 (your_email@example.com)"
SLEEP_BETWEEN_REQUESTS = 0.5   # seconds between API requests (increase for large crawls)
RETRY_WAIT = 5                 # seconds to wait on HTTP errors
MAX_RETRIES = 3
# ---------------------------------------------

def mw_api_endpoint(lang):
    return f"https://{lang}.wikipedia.org/w/api.php"

def api_get(session, url, params):
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            else:
                print(f"HTTP {resp.status_code} from {url} with params {params}. Retrying in {RETRY_WAIT}s...")
                time.sleep(RETRY_WAIT)
        except requests.RequestException as e:
            print(f"Request failed: {e}. Retrying in {RETRY_WAIT}s...")
            time.sleep(RETRY_WAIT)
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} retries.")

def get_category_members(session, api_url, category, cmtype="page|subcat", limit=500):
    """
    Generator yields category members for a single category.
    cmtype: "page", "subcat" or "page|subcat"
    """
    params = {
        "action": "query",
        "format": "json",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmlimit": limit,
        "cmtype": cmtype,
    }
    cont = {}
    while True:
        params.update(cont)
        data = api_get(session, api_url, params)
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            yield m
        if "continue" in data:
            cont = data["continue"]
        else:
            break

def get_page_extract(session, api_url, pageid=None, title=None):
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": 1,
        "exlimit": "1",
    }
    if pageid is not None:
        params["pageids"] = pageid
    elif title is not None:
        params["titles"] = title
    else:
        raise ValueError("pageid or title required")
    data = api_get(session, api_url, params)
    pages = data.get("query", {}).get("pages", {})
    # pages is dict keyed by pageid
    for pid, p in pages.items():
        # skip missing
        if str(pid).startswith("-"):
            return None
        extract = p.get("extract", "")
        return {
            "pageid": p.get("pageid"),
            "title": p.get("title"),
            "extract": extract,
        }
    return None

def crawl_category(session, api_url, out_fh, category, lang, max_pages_per_cat=500, depth=1, seen_pages=None, category_path=None):
    """
    Recursively crawl a category, yield page extracts.
    - depth: recursion depth for subcategories (depth=0 means don't traverse subcategories)
    """
    if seen_pages is None:
        seen_pages = set()
    if category_path is None:
        category_path = [category]

    print(f"[{lang}] Crawling Category: {' > '.join(category_path)} (depth={depth})")

    pages_count = 0

    # First collect pages in this category
    for member in get_category_members(session, api_url, category, cmtype="page", limit=500):
        if pages_count >= max_pages_per_cat:
            break
        pageid = member["pageid"]
        title = member["title"]
        if pageid in seen_pages:
            continue
        try:
            info = get_page_extract(session, api_url, pageid=pageid)
        except Exception as e:
            print(f"Failed to fetch page {title}: {e}")
            continue
        if not info or not info["extract"].strip():
            continue
        seen_pages.add(pageid)
        pages_count += 1

        out_obj = {
            "lang": lang,
            "category_path": category_path.copy(),
            "title": info["title"],
            "pageid": info["pageid"],
            "text": info["extract"],
            "url": f"https://{lang}.wikipedia.org/wiki/{quote(info['title'].replace(' ', '_'))}"
        }
        out_fh.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
        # be polite
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Then, if allowed, recurse into subcategories
    if depth > 0:
        for member in get_category_members(session, api_url, category, cmtype="subcat", limit=500):
            subcat_title = member["title"].replace("Category:", "")
            # Avoid cycles: if subcat is same as parent, skip
            if subcat_title in category_path:
                continue
            # Recurse
            crawl_category(session, api_url, out_fh, subcat_title, lang,
                           max_pages_per_cat=max_pages_per_cat,
                           depth=depth-1,
                           seen_pages=seen_pages,
                           category_path=category_path + [subcat_title])

def main(langs, categories, outpath="wiki_corpus.jsonl", max_pages=20, depth=1):
    session = requests.Session()
    # Open output file
    with open(outpath, "w", encoding="utf-8") as out_fh:
        for lang in langs:
            api_url = mw_api_endpoint(lang)
            for cat in categories:
                # For each top-level category, crawl recursively
                try:
                    crawl_category(session, api_url, out_fh, cat, lang,
                                   max_pages_per_cat=max_pages, depth=depth)
                except Exception as e:
                    print(f"Error crawling {lang}:{cat} -> {e}")
                    time.sleep(RETRY_WAIT)

if __name__ == "__main__":

      print("Starting crawl with:", )
      main(["en", "sr", "zh"], ["Physics", "Mathematics", "Computer_science"], outpath="../data/wiki_corpus.jsonl", max_pages = 60, depth = 2)
