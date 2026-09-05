"""
Monthly news snapshot for the EndoFind dashboard.

Pulls Google News RSS (same queries the v6 dashboard used) and writes
data/news.json. The GitHub Pages dashboards load that file on each visit.

Run: python scripts/update_endo_news.py
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

NEWS_PATH = Path("data/news.json")
PER_SECTION = 8
TIMEOUT = 20

FEEDS = {
    "endo": "https://news.google.com/rss/search?q=endometriosis+excision+surgery+treatment&hl=en-US&gl=US&ceid=US:en",
    "womens": "https://news.google.com/rss/search?q=women+gynecology+health+pelvic+pain&hl=en-US&gl=US&ceid=US:en",
    "fertility": "https://news.google.com/rss/search?q=endometriosis+fertility+IVF+treatment&hl=en-US&gl=US&ceid=US:en",
    "medical": "https://news.google.com/rss/search?q=endometriosis+medical+breakthrough+surgery&hl=en-US&gl=US&ceid=US:en",
    "heartwarming": "https://news.google.com/rss/search?q=endometriosis+patient+story+recovery&hl=en-US&gl=US&ceid=US:en",
}

HEADERS = {
    "User-Agent": "EndoFind News Bot/1.0 (educational health resource; GitHub Pages snapshot)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

TAG_RE = re.compile(r"<[^>]+>")


def strip_html(s: str) -> str:
    return unescape(TAG_RE.sub("", s or "")).strip()


def load_existing() -> dict:
    if NEWS_PATH.exists():
        try:
            return json.loads(NEWS_PATH.read_text())
        except Exception as e:
            log.warning("Could not read existing news.json: %s", e)
    return {"status": "ok", "updated": "", "sections": {}}


def parse_rss(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    items = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        source = item.find("source")
        author = (source.text if source is not None and source.text else "") or (
            item.findtext("author") or "Google News"
        )
        items.append(
            {
                "title": title,
                "link": link,
                "description": strip_html(item.findtext("description") or "")[:280],
                "pubDate": (item.findtext("pubDate") or "").strip(),
                "author": author.strip()[:60],
            }
        )
        if len(items) >= PER_SECTION:
            break
    return items


def fetch_section(url: str) -> list[dict]:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return parse_rss(r.text)


def main() -> None:
    existing = load_existing()
    sections = dict(existing.get("sections") or {})
    results = {}

    for key, url in FEEDS.items():
        try:
            items = fetch_section(url)
            if items:
                sections[key] = items
                results[key] = len(items)
                log.info("%s: %s stories", key, len(items))
            else:
                results[key] = 0
                log.warning("%s: empty feed, keeping previous snapshot", key)
        except Exception as e:
            results[key] = f"error: {e}"
            log.error("%s failed: %s", key, e)

    payload = {
        "status": "ok",
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "note": "Monthly snapshot from Google News RSS. Not a live feed.",
        "sections": sections,
        "fetch": results,
    }
    NEWS_PATH.parent.mkdir(parents=True, exist_ok=True)
    NEWS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    log.info("Wrote %s", NEWS_PATH)


if __name__ == "__main__":
    main()
