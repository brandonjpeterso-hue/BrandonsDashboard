"""
EndoFind Monthly Surgeon Data Updater
Scrapes vetted endometriosis excision surgeon directories and merges
new entries into data/surgeons.json without overwriting existing data.

Sources:
  1. iCareBetter — video-vetted excision surgeons
  2. EndofEndo Project — patient-rated excision specialists
  3. Pelvic Rehabilitation Medicine — fellowship-trained list
  4. Yellow Hub (static page) — patient-recommended map data

Run: python scripts/update_surgeons.py
"""

import json
import time
import logging
import re
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'EndoFind Research Bot/1.0 (educational health resource; contact via GitHub)',
    'Accept': 'text/html,application/xhtml+xml',
}
TIMEOUT = 15
SLEEP = 2  # seconds between requests — be polite

DATA_PATH = Path('data/surgeons.json')
LOG_PATH = Path('data/update_log.json')


# ── HELPERS ──────────────────────────────────────────────────────────────────

def get(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return parsed BeautifulSoup, or None on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        time.sleep(SLEEP)
        return BeautifulSoup(r.text, 'lxml')
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
        return None


def normalize_name(name: str) -> str:
    """Normalize a doctor name for deduplication."""
    return re.sub(r'\s+', ' ', name.strip().lower()
                  .replace('dr. ', '').replace('dr ', '')
                  .replace('.', '').replace(',', ''))


def make_id(name: str) -> str:
    """Create a URL-safe ID from a name."""
    n = normalize_name(name)
    return re.sub(r'[^a-z0-9]+', '-', n).strip('-')


# ── SCRAPERS ─────────────────────────────────────────────────────────────────

def scrape_icarebetter() -> list[dict]:
    """
    Scrape iCareBetter excision surgeon directory.
    Paginated at /endometriosis/specialist/page/N/
    Returns list of partial surgeon dicts.
    """
    surgeons = []
    base = 'https://icarebetter.com/endometriosis/specialist'
    page = 1

    while True:
        url = base if page == 1 else f'{base}/page/{page}/'
        log.info(f"iCareBetter page {page}: {url}")
        soup = get(url)
        if not soup:
            break

        # Find doctor cards — update selector if site HTML changes
        cards = soup.select('div.doctor-card, article.doctor, div.specialist-card, div[class*="doctor"]')

        if not cards:
            # Try generic approach — look for h2/h3 with "Dr."
            cards = []
            for tag in soup.find_all(['h2', 'h3', 'h4']):
                if tag.text.strip().startswith('Dr.'):
                    cards.append(tag.parent)

        if not cards:
            log.info(f"No cards found on page {page}, stopping.")
            break

        for card in cards:
            name_tag = card.find(['h2', 'h3', 'h4', 'a'])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not name.startswith('Dr.'):
                continue

            # Try to extract location
            loc_tag = card.find(class_=re.compile(r'location|city|state', re.I))
            location = loc_tag.get_text(strip=True) if loc_tag else ''

            # Try to extract profile link
            link_tag = card.find('a', href=True)
            profile = link_tag['href'] if link_tag else ''

            surgeons.append({
                'name': name,
                'source_location': location,
                'source': 'iCareBetter',
                'profile_url': profile,
                'specs': ['Excision Surgery'],
                'accepting': True,
            })

        # Check for next page
        next_btn = soup.select_one('a.next, a[rel="next"], .pagination .next')
        if not next_btn:
            break
        page += 1
        if page > 40:  # Safety limit
            break

    log.info(f"iCareBetter: found {len(surgeons)} entries")
    return surgeons


def scrape_endofendo() -> list[dict]:
    """
    Scrape EndofEndo Project physician directory.
    https://endofendoproject.org/physician-directory
    """
    surgeons = []
    url = 'https://endofendoproject.org/physician-directory'
    log.info(f"EndofEndo: {url}")
    soup = get(url)
    if not soup:
        return surgeons

    # Look for directory entries
    entries = soup.select('div.physician, div.doctor, div[class*="physician"], div[class*="doctor"]')
    if not entries:
        # Fallback: find any element containing "Dr."
        entries = [el.parent for el in soup.find_all(string=re.compile(r'Dr\. \w'))]
        entries = list(set(entries))[:100]

    for entry in entries:
        text = entry.get_text(separator=' ', strip=True)
        name_match = re.search(r'(Dr\. [A-Z][a-z]+(?: [A-Z][a-z]+)+)', text)
        if not name_match:
            continue
        name = name_match.group(1)

        # Try to extract phone
        phone_match = re.search(r'\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}', text)
        phone = phone_match.group(0) if phone_match else None

        # Try to extract city/state
        loc_match = re.search(r'([A-Z][a-z]+(?: [A-Z][a-z]+)*),\s*([A-Z]{2})', text)
        city = loc_match.group(1) if loc_match else ''
        state = loc_match.group(2) if loc_match else ''

        link_tag = entry.find('a', href=re.compile(r'http'))
        web = link_tag['href'] if link_tag else None

        surgeons.append({
            'name': name,
            'city': city,
            'state': state,
            'phone': phone,
            'web': web,
            'source': 'EndofEndo',
            'specs': ['Excision Surgery'],
            'accepting': True,
        })

    log.info(f"EndofEndo: found {len(surgeons)} entries")
    return surgeons


def scrape_pelvic_rehab() -> list[dict]:
    """
    Scrape Pelvic Rehabilitation Medicine static surgeon list.
    https://pelvicrehabilitation.com/nationwide-endometriosis-excision-surgeons
    """
    surgeons = []
    url = 'https://pelvicrehabilitation.com/nationwide-endometriosis-excision-surgeons'
    log.info(f"Pelvic Rehab: {url}")
    soup = get(url)
    if not soup:
        return surgeons

    # Find all "Dr." mentions
    for el in soup.find_all(string=re.compile(r'Dr\. [A-Z]')):
        name_match = re.search(r'(Dr\. [A-Z][a-z]+(?: [A-Z][a-z]+)+)', el)
        if not name_match:
            continue
        name = name_match.group(1)
        parent_text = el.parent.get_text(separator=' ', strip=True) if el.parent else ''

        loc_match = re.search(r'([A-Z][a-z]+(?: [A-Z][a-z]+)*),\s*([A-Z]{2}|\w+)', parent_text)
        city = loc_match.group(1) if loc_match else ''
        state = loc_match.group(2) if loc_match else ''

        surgeons.append({
            'name': name,
            'city': city,
            'state': state,
            'phone': None,
            'web': 'https://pelvicrehabilitation.com/request-an-appointment/',
            'source': 'Pelvic Rehabilitation Medicine',
            'specs': ['Excision Surgery'],
            'accepting': True,
            'notes': 'Fellowship-trained excision surgeon listed by Pelvic Rehabilitation Medicine.',
        })

    log.info(f"Pelvic Rehab: found {len(surgeons)} entries")
    return surgeons


# ── MERGE LOGIC ──────────────────────────────────────────────────────────────

def merge_surgeons(existing: list[dict], new_entries: list[dict]) -> tuple[list[dict], int]:
    """
    Merge new scraped entries into existing list.
    Deduplicates by normalized name.
    Returns (merged_list, count_added).
    """
    existing_names = {normalize_name(d['name']) for d in existing}
    added = 0

    for entry in new_entries:
        name = entry.get('name', '').strip()
        if not name or not name.startswith('Dr.'):
            continue
        norm = normalize_name(name)
        if norm in existing_names:
            continue  # Already have this surgeon

        # Build a minimal record for new additions
        record = {
            'id': make_id(name),
            'name': name,
            'fn': name.replace('Dr. ', '').split()[0] if name.startswith('Dr. ') else '',
            'ln': name.replace('Dr. ', '').split()[-1] if name.startswith('Dr. ') else '',
            'creds': ['MD'],
            'org': entry.get('org', 'Verify directly'),
            'city': entry.get('city', ''),
            'state': entry.get('state', ''),
            'lat': None,
            'lon': None,
            'phone': entry.get('phone'),
            'web': entry.get('web') or entry.get('profile_url', ''),
            'specs': entry.get('specs', ['Excision Surgery']),
            'ins': 'Verify directly',
            'accepting': entry.get('accepting', True),
            'notes': entry.get('notes', f"Added via {entry.get('source', 'monthly update')}. Verify details directly."),
            'stars': 4,
            'source': entry.get('source', 'Monthly Update'),
            'verified': False,  # Flag for manual review
        }
        existing.append(record)
        existing_names.add(norm)
        added += 1
        log.info(f"  + Added: {name} ({entry.get('source', '?')})")

    return existing, added


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== EndoFind Monthly Surgeon Update ===")

    # Load existing data
    if DATA_PATH.exists():
        with open(DATA_PATH) as f:
            existing = json.load(f)
        log.info(f"Loaded {len(existing)} existing surgeons")
    else:
        existing = []
        log.info("No existing data — starting fresh")

    total_added = 0
    results_by_source = {}

    # Run scrapers
    scrapers = [
        ('iCareBetter', scrape_icarebetter),
        ('EndofEndo', scrape_endofendo),
        ('Pelvic Rehab', scrape_pelvic_rehab),
    ]

    for source_name, scraper_fn in scrapers:
        try:
            log.info(f"\n--- Scraping {source_name} ---")
            new_entries = scraper_fn()
            existing, added = merge_surgeons(existing, new_entries)
            total_added += added
            results_by_source[source_name] = {
                'scraped': len(new_entries),
                'added': added,
            }
            log.info(f"{source_name}: scraped {len(new_entries)}, added {added} new")
        except Exception as e:
            log.error(f"{source_name} failed: {e}")
            results_by_source[source_name] = {'error': str(e)}

    # Sort by state, then last name
    existing.sort(key=lambda d: (d.get('state', ''), d.get('ln', '').lower()))

    # Save updated surgeon list
    with open(DATA_PATH, 'w') as f:
        json.dump(existing, f, indent=2)
    log.info(f"\nSaved {len(existing)} total surgeons to {DATA_PATH}")

    # Save update log
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'total_surgeons': len(existing),
        'added_this_run': total_added,
        'sources': results_by_source,
    }
    history = []
    if LOG_PATH.exists():
        with open(LOG_PATH) as f:
            history = json.load(f)
    history.append(log_entry)
    history = history[-24:]  # Keep last 24 months
    with open(LOG_PATH, 'w') as f:
        json.dump(history, f, indent=2)

    log.info(f"Update complete. Added {total_added} new surgeons.")
    log.info("NOTE: New entries marked verified=false — review before promoting to main list.")


if __name__ == '__main__':
    main()
