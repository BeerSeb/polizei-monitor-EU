#!/usr/bin/env python3
"""
DE Polizei OSINT Monitor – Scraper
====================================
Scrapt Pressemitteilungen aller 16 Landespolizeien + Bundespolizei,
geocodiert Ortsangaben via Nominatim und speichert incidents.json.

GitHub Actions: täglich 07:00 UTC
"""

import json
import time
import re
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("polizei-de-scraper")

OUTPUT_FILE = Path("data/incidents.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

# ── GEOCODING ────────────────────────────────────────────────────────────────
GEO_CACHE: dict = {}
GEO_CACHE_FILE = Path("data/geo_cache.json")

def load_geo_cache():
    global GEO_CACHE
    if GEO_CACHE_FILE.exists():
        GEO_CACHE = json.loads(GEO_CACHE_FILE.read_text(encoding="utf-8"))
        log.info(f"Geo-Cache geladen: {len(GEO_CACHE)} Einträge")

def save_geo_cache():
    GEO_CACHE_FILE.write_text(json.dumps(GEO_CACHE, ensure_ascii=False), encoding="utf-8")

def geocode(location: str, bundesland: str) -> tuple[Optional[float], Optional[float]]:
    """Nominatim-Geocoding mit Cache und Rate-Limit (1 req/s)."""
    key = f"{location}|{bundesland}"
    if key in GEO_CACHE:
        return GEO_CACHE[key]

    query = f"{location}, {bundesland}, Deutschland"
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "de"},
            headers={"User-Agent": "DE-Polizei-OSINT-Monitor/1.0 (github.com)"},
            timeout=10,
        )
        time.sleep(1.1)  # Nominatim Rate-Limit einhalten
        data = resp.json()
        if data:
            lat, lng = float(data[0]["lat"]), float(data[0]["lon"])
            GEO_CACHE[key] = (lat, lng)
            save_geo_cache()
            return lat, lng
    except Exception as e:
        log.warning(f"Geocoding fehlgeschlagen für '{query}': {e}")

    GEO_CACHE[key] = (None, None)
    return None, None


# ── KATEGORISIERUNG ───────────────────────────────────────────────────────────
KATEGORIE_KEYWORDS = {
    "Unfall":            ["unfall", "kollision", "verunglückt", "crash"],
    "Einbruch":          ["einbruch", "eingebrochen", "einbrecher", "einbruchsversuch"],
    "Diebstahl":         ["diebstahl", "gestohlen", "entwendet", "taschendieb"],
    "Körperverletzung":  ["körperverletzung", "schlägerei", "angegriffen", "geschlagen", "verletzt"],
    "Betrug":            ["betrug", "betrüger", "phishing", "enkeltrick", "falsche polizei"],
    "Drogen":            ["drogen", "betäubungsmittel", "rauschgift", "kokain", "heroin", "cannabis"],
    "Verkehr":           ["trunkenheit", "alkohol am steuer", "führerflucht", "verkehrskontrolle", "geisterfahrer"],
    "Vermisstenfall":    ["vermisst", "vermisstenfall", "gesucht", "hilfe gefunden"],
    "Brand":             ["brand", "feuer", "flammen", "rauch", "feuerwehr"],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIE_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"


# ── ORT EXTRAHIEREN ───────────────────────────────────────────────────────────
ORT_PATTERN = re.compile(
    r"\b(in|bei|aus|im Bereich|Stadteil|Ortsteil|Gemeinde|Stadt|Kreis|Landkreis)\s+"
    r"([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:\s[A-ZÄÖÜ][a-zäöüß\-]{2,})*)",
    re.UNICODE,
)

def extrahiere_ort(text: str) -> str:
    m = ORT_PATTERN.search(text)
    return m.group(2) if m else ""


def incident_id(url: str, title: str) -> str:
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:12]


# ── QUELLEN ───────────────────────────────────────────────────────────────────
# Jede Quelle: name, bundesland, rss (bevorzugt) oder scrape-Funktion
SOURCES = [
    # Bundespolizei
    {
        "bundesland": "Bundespolizei",
        "rss": "https://www.bundespolizei.de/Web/DE/04Aktuelles/01Meldungen/meldungen_node.html?view=renderRSS",
    },
    # Baden-Württemberg
    {
        "bundesland": "Baden-Württemberg",
        "rss": "https://www.polizei-bw.de/rss-feeds/pressemitteilungen.rss",
    },
    # Bayern
    {
        "bundesland": "Bayern",
        "rss": "https://www.polizei.bayern.de/aktuelles/pressemitteilungen/index.html/rss",
    },
    # Berlin
    {
        "bundesland": "Berlin",
        "rss": "https://www.berlin.de/polizei/polizeimeldungen/rss.php",
    },
    # Brandenburg
    {
        "bundesland": "Brandenburg",
        "rss": "https://www.polizei.brandenburg.de/pressemitteilungen/?type=9818",
    },
    # Bremen
    {
        "bundesland": "Bremen",
        "rss": "https://www.presseportal.de/rss/pid_6337.rss2",
    },
    # Hamburg
    {
        "bundesland": "Hamburg",
        "rss": "https://www.presseportal.de/rss/pid_6337.rss2",  # Placeholder – Hamburg Polizei Presseportal
    },
    # Hessen
    {
        "bundesland": "Hessen",
        "rss": "https://www.polizei.hessen.de/presse/Pressemitteilungen/?view=rss",
    },
    # Mecklenburg-Vorpommern
    {
        "bundesland": "Mecklenburg-Vorpommern",
        "rss": "https://www.presseportal.de/rss/pid_6200.rss2",
    },
    # Niedersachsen
    {
        "bundesland": "Niedersachsen",
        "rss": "https://www.pd-h.polizei-nds.de/dienststellen/presse_und_oeffentlichkeitsarbeit/rss.xml",
    },
    # NRW
    {
        "bundesland": "Nordrhein-Westfalen",
        "rss": "https://www.presseportal.de/rss/pid_6335.rss2",
    },
    # Rheinland-Pfalz
    {
        "bundesland": "Rheinland-Pfalz",
        "rss": "https://www.presseportal.de/rss/pid_6339.rss2",
    },
    # Saarland
    {
        "bundesland": "Saarland",
        "rss": "https://www.presseportal.de/rss/pid_6334.rss2",
    },
    # Sachsen
    {
        "bundesland": "Sachsen",
        "rss": "https://www.presseportal.de/rss/pid_6337.rss2",
    },
    # Sachsen-Anhalt
    {
        "bundesland": "Sachsen-Anhalt",
        "rss": "https://www.presseportal.de/rss/pid_6338.rss2",
    },
    # Schleswig-Holstein
    {
        "bundesland": "Schleswig-Holstein",
        "rss": "https://www.presseportal.de/rss/pid_6341.rss2",
    },
    # Thüringen
    {
        "bundesland": "Thüringen",
        "rss": "https://www.presseportal.de/rss/pid_6343.rss2",
    },
]


# ── RSS PARSER ────────────────────────────────────────────────────────────────
def parse_rss(url: str, bundesland: str, max_items: int = 50) -> list[dict]:
    """Liest einen RSS-Feed und gibt normalisierte Incident-Dicts zurück."""
    incidents = []
    try:
        resp = requests.get(url, headers={"User-Agent": "DE-Polizei-OSINT-Monitor/1.0"}, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "xml")

        items = soup.find_all("item")[:max_items]
        log.info(f"  {bundesland}: {len(items)} Items gefunden")

        for item in items:
            title = item.find("title")
            title = title.get_text(strip=True) if title else ""
            link  = item.find("link")
            link  = link.get_text(strip=True) if link else ""
            desc  = item.find("description") or item.find("summary")
            desc  = desc.get_text(strip=True) if desc else ""
            pub   = item.find("pubDate") or item.find("published") or item.find("dc:date")
            pub   = pub.get_text(strip=True) if pub else ""

            if not title:
                continue

            # Datum parsen
            date_iso = ""
            for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"]:
                try:
                    date_iso = datetime.strptime(pub.strip(), fmt).isoformat()
                    break
                except Exception:
                    pass
            if not date_iso:
                date_iso = datetime.now(timezone.utc).isoformat()

            full_text = f"{title} {desc}"
            ort = extrahiere_ort(full_text) or bundesland
            kat = kategorisieren(full_text)

            incidents.append({
                "id":         incident_id(link, title),
                "title":      title,
                "summary":    desc[:500] if desc else "",
                "category":   kat,
                "bundesland": bundesland,
                "location":   ort,
                "lat":        None,
                "lng":        None,
                "date":       date_iso,
                "url":        link,
                "source":     url.split("/")[2],  # domain
            })

    except Exception as e:
        log.error(f"  Fehler bei {bundesland} ({url}): {e}")

    return incidents


# ── GEOCODING PASS ────────────────────────────────────────────────────────────
def geocode_incidents(incidents: list[dict]) -> list[dict]:
    """Geocodiert alle Incidents ohne Koordinaten."""
    need = [i for i in incidents if i["lat"] is None and i["location"]]
    log.info(f"Geocoding: {len(need)} Orte werden aufgelöst …")

    for inc in need:
        lat, lng = geocode(inc["location"], inc["bundesland"])
        inc["lat"] = lat
        inc["lng"] = lng

    return incidents


# ── DEDUPLICATION ─────────────────────────────────────────────────────────────
def deduplicate(incidents: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for inc in incidents:
        if inc["id"] not in seen:
            seen.add(inc["id"])
            result.append(inc)
    return result


# ── HAUPTFUNKTION ─────────────────────────────────────────────────────────────
def main():
    load_geo_cache()

    # Bestehende Daten laden (für Geo-Cache-Effizienz)
    existing = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            log.info(f"Bestehende Daten: {len(existing)} Incidents")
        except Exception:
            pass

    existing_ids = {e["id"] for e in existing}

    # Alle Quellen scrapen
    all_new = []
    for src in SOURCES:
        bundesland = src["bundesland"]
        log.info(f"Scraping: {bundesland} …")
        items = parse_rss(src["rss"], bundesland)
        # Nur neue Items geocodieren
        new_items = [i for i in items if i["id"] not in existing_ids]
        log.info(f"  → {len(new_items)} neue Incidents")
        all_new.extend(new_items)
        time.sleep(0.5)

    # Geocoding nur für neue Incidents
    if all_new:
        geocode_incidents(all_new)

    # Zusammenführen, deduplicieren, sortieren
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)

    # Auf 90 Tage begrenzen
    cutoff = datetime.now(timezone.utc).timestamp() - 90 * 86400
    combined = [
        i for i in combined
        if i.get("date") and
        datetime.fromisoformat(i["date"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]

    OUTPUT_FILE.write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8")
    geo_count = sum(1 for i in combined if i["lat"])
    log.info(f"✓ {len(combined)} Incidents gespeichert ({geo_count} georeferenziert)")


if __name__ == "__main__":
    main()
