#!/usr/bin/env python3
"""
Europa Polizei OSINT Monitor – Scraper v3
==========================================
Nur verifizierte, funktionierende Quellen.
Fehlerhafte URLs aus dem Log wurden korrigiert oder ersetzt.

Fixes gegenüber v2:
- Presseportal: /rss/pid_XXXX.rss2 → /blaulicht/r/BUNDESLAND.rss2 (neue URL-Struktur)
- Bundespolizei: eigener RSS weg → presseportal nr/73990
- Bayern: direkter RSS weg → presseportal /blaulicht/r/Bayern.rss2
- Berlin: rss.php weg → presseportal /blaulicht/r/Berlin.rss2
- BW, Hessen: 404 → presseportal Bundesland-Feeds
- Schweiz: SSL-Fehler → verify=False + neue URLs
- Österreich: leere Items → HTML-Scraping polizei.gv.at
- UK Met Police: 404 → news.met.police.uk/feed/
- GMP, Thames Valley: 403 → entfernt, durch andere UK-Quellen ersetzt
- Polen, Niederlande: funktionieren, URLs beibehalten
"""

import json
import time
import re
import hashlib
import logging
import ssl
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("europa-scraper-v3")

OUTPUT_FILE    = Path("data/incidents.json")
GEO_CACHE_FILE = Path("data/geo_cache.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Europa-Polizei-OSINT-Monitor/3.0; +https://github.com/BeerSeb/polizei-monitor-de)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "de,en;q=0.9",
}

# ── GEO CACHE ─────────────────────────────────────────────────────────────────
GEO_CACHE: dict = {}

def load_geo_cache():
    global GEO_CACHE
    if GEO_CACHE_FILE.exists():
        GEO_CACHE = json.loads(GEO_CACHE_FILE.read_text(encoding="utf-8"))
        log.info(f"Geo-Cache: {len(GEO_CACHE)} Einträge")

def save_geo_cache():
    GEO_CACHE_FILE.write_text(json.dumps(GEO_CACHE, ensure_ascii=False, indent=2), encoding="utf-8")

def geocode(location: str, country: str) -> tuple[Optional[float], Optional[float]]:
    if not location or len(location.strip()) < 3:
        return None, None
    key = f"{location}|{country}"
    if key in GEO_CACHE:
        return GEO_CACHE[key]
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{location}, {country}", "format": "json", "limit": 1},
            headers={"User-Agent": "Europa-Polizei-OSINT-Monitor/3.0"},
            timeout=10,
        )
        time.sleep(1.1)
        data = r.json()
        if data:
            result = (float(data[0]["lat"]), float(data[0]["lon"]))
            GEO_CACHE[key] = result
            save_geo_cache()
            return result
    except Exception as e:
        log.warning(f"Geocoding '{location}': {e}")
    GEO_CACHE[key] = (None, None)
    return None, None


# ── KATEGORISIERUNG ───────────────────────────────────────────────────────────
KATEGORIEN = {
    "Unfall":            ["unfall","kollision","verunglückt","accident","accidente","crash","botsing","wypadek","nehoda"],
    "Einbruch":          ["einbruch","eingebrochen","cambriolage","burglary","inbraak","wlamanie","vloupání","effraction"],
    "Diebstahl":         ["diebstahl","gestohlen","vol ","hurto","theft","diefstal","kradzież","krádež","furto"],
    "Körperverletzung":  ["körperverletzung","schlägerei","agression","assault","mishandeling","pobicie","aggressione"],
    "Betrug":            ["betrug","phishing","fraude","fraud","oplichting","oszustwo","podvod","truffa"],
    "Drogen":            ["drogen","drogue","droga","drugs","narkotyki","cannabis","kokain","cocaïne","heroin"],
    "Verkehr":           ["trunkenheit","alkohol am steuer","tráfico","traffic","rijden onder invloed","vitesse"],
    "Vermisstenfall":    ["vermisst","disparition","desaparecido","missing","vermist","zaginięcie","scomparsa"],
    "Brand":             ["brand","feuer","incendie","incendio","fire","pożar","požár","arson"],
    "Festnahme":         ["festgenommen","verhaftet","arrestation","arrested","aangehouden","zatrzymany","arrestato"],
    "Raub":              ["raub","überfall","robbery","braquage","robo","rapina","overval","napad"],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIEN.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"


# ── ORT EXTRAHIEREN ───────────────────────────────────────────────────────────
ORT_RE = re.compile(
    r"\b(?:in|bei|aus|in der Stadt|Gemeinde|Stadt|Kreis|at|near|à|au|en|in)\s+"
    r"([A-ZÄÖÜÀ-Ö][a-zäöüà-öß\-]{2,}(?:[\s\-][A-ZÄÖÜÀ-Ö][a-zäöüà-öß\-]{2,})?)",
    re.UNICODE
)

def extrahiere_ort(text: str) -> str:
    m = ORT_RE.search(text)
    if m:
        ort = m.group(1).strip()
        if 3 <= len(ort) <= 50:
            return ort
    return ""

def incident_id(url: str, title: str) -> str:
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:12]

def parse_date(raw: str) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    raw = raw.strip()
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
        "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(raw, fmt).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ── RSS PARSER ────────────────────────────────────────────────────────────────
def parse_rss(url: str, land: str, country: str, max_items: int = 50,
              verify_ssl: bool = True) -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=verify_ssl,
                        allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "xml")
        items = soup.find_all("item")[:max_items]
        if not items:
            items = soup.find_all("entry")[:max_items]
        log.info(f"  {land}: {len(items)} Items")

        for item in items:
            title = item.find("title")
            title = title.get_text(strip=True) if title else ""
            link  = item.find("link")
            link  = (link.get("href") or link.get_text(strip=True)) if link else ""
            desc  = item.find("description") or item.find("summary") or item.find("content")
            desc_text = BeautifulSoup(desc.get_text(strip=True), "html.parser").get_text() if desc else ""
            pub   = item.find("pubDate") or item.find("published") or item.find("dc:date")
            pub   = pub.get_text(strip=True) if pub else ""

            if not title or len(title) < 5:
                continue

            full = f"{title} {desc_text}"
            incidents.append({
                "id":       incident_id(link, title),
                "title":    title[:220],
                "summary":  desc_text[:700],
                "category": kategorisieren(full),
                "land":     land,
                "country":  country,
                "location": extrahiere_ort(full) or land,
                "lat":      None, "lng": None,
                "date":     parse_date(pub),
                "url":      link,
                "source":   url.split("/")[2],
            })
    except Exception as e:
        log.error(f"  FEHLER {land}: {e}")
    return incidents


# ── ÖSTERREICH HTML SCRAPER ───────────────────────────────────────────────────
def scrape_austria() -> list[dict]:
    """polizei.gv.at hat keinen funktionierenden RSS – HTML-Scraping."""
    incidents = []
    urls = [
        ("https://www.polizei.gv.at/wien/presse/aussendungen/presse.html", "Wien"),
        ("https://www.polizei.gv.at/ooe/presse/aussendungen/presse.aspx", "Oberösterreich"),
        ("https://www.polizei.gv.at/sbg/presse/aussendungen/presse.aspx", "Salzburg"),
        ("https://www.polizei.gv.at/stmk/presse/aussendungen/presse.aspx", "Steiermark"),
        ("https://www.polizei.gv.at/tirol/presse/aussendungen/presse.aspx", "Tirol"),
    ]
    for url, region in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "html.parser")

            # polizei.gv.at Struktur: table rows oder div-Blöcke
            rows = soup.select("table tr, .pressemeldung, .aussendung, article")[:30]
            for row in rows:
                cells = row.find_all("td")
                if cells:
                    date_raw = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                    title    = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    link_el  = row.find("a")
                    link     = ("https://www.polizei.gv.at" + link_el["href"]) if link_el and link_el.get("href","").startswith("/") else (link_el["href"] if link_el else "")
                else:
                    title_el = row.find(["h2","h3","a"])
                    title    = title_el.get_text(strip=True) if title_el else row.get_text(strip=True)[:100]
                    link_el  = row.find("a")
                    link     = link_el.get("href","") if link_el else ""
                    date_raw = ""

                if not title or len(title) < 5:
                    continue
                incidents.append({
                    "id":       incident_id(link, title),
                    "title":    title[:220],
                    "summary":  "",
                    "category": kategorisieren(title),
                    "land":     region,
                    "country":  "Österreich",
                    "location": extrahiere_ort(title) or region,
                    "lat":      None, "lng": None,
                    "date":     parse_date(date_raw),
                    "url":      link,
                    "source":   "polizei.gv.at",
                })
            log.info(f"  Österreich/{region}: {len(incidents)} gesamt")
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  Österreich/{region}: {e}")
    return incidents


# ── QUELLEN – NUR VERIFIZIERTE URLs ──────────────────────────────────────────
#
# Presseportal-Struktur (neu, Stand 2025/2026):
#   https://www.presseportal.de/blaulicht/r/BUNDESLAND.rss2   → Bundesland-Feed
#   https://www.presseportal.de/blaulicht/nr/NUMMER.rss2      → Dienststellen-Feed
#
# Aus dem Dienststellen-Verzeichnis (presseportal.de/blaulicht/dienststellen):
#   Bundespolizeipräsidium: nr/73990
#   BKA: nr/7
#   Bayern PP München: nr/19136  PP Oberbayern Nord: nr/101170
#   Berlin: nr/9358
#   NRW Köln: nr/12415  Düsseldorf: nr/59488  Dortmund: nr/11851
#   BW Karlsruhe: nr/110972  Stuttgart: nr/110975  Freiburg: nr/110970
#   Hessen: nr/168771 (LKA Hessen)
#   Niedersachsen Hannover: nr/66841
#   Sachsen Dresden: nr/13013
#   Hamburg: nr/6337
#   Schleswig-Holstein: nr/6337  → eigene Seite
#   Thüringen Erfurt: nr/126725

SOURCES_RSS = [
    # ── DEUTSCHLAND: Presseportal Bundesland-Feeds (neue URL-Struktur) ────────
    {"land": "Deutschland",          "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Deutschland.rss2"},
    {"land": "Bayern",               "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Bayern.rss2"},
    {"land": "Berlin",               "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Berlin.rss2"},
    {"land": "Baden-Württemberg",    "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Baden-W%C3%BCrttemberg.rss2"},
    {"land": "NRW",                  "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Nordrhein-Westfalen.rss2"},
    {"land": "Hessen",               "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Hessen.rss2"},
    {"land": "Niedersachsen",        "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Niedersachsen.rss2"},
    {"land": "Hamburg",              "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Hamburg.rss2"},
    {"land": "Sachsen",              "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Sachsen.rss2"},
    {"land": "Brandenburg",          "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Brandenburg.rss2"},
    {"land": "Thüringen",            "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Th%C3%BCringen.rss2"},
    {"land": "Sachsen-Anhalt",       "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Sachsen-Anhalt.rss2"},
    {"land": "Schleswig-Holstein",   "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Schleswig-Holstein.rss2"},
    {"land": "Rheinland-Pfalz",      "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Rheinland-Pfalz.rss2"},
    {"land": "Saarland",             "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Saarland.rss2"},
    {"land": "Mecklenburg-Vorpommern","country":"Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Mecklenburg-Vorpommern.rss2"},
    {"land": "Bremen",               "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Bremen.rss2"},
    # Bundespolizei über Presseportal (nr/73990 = Bundespolizeipräsidium)
    {"land": "Bundespolizei",        "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/nr/73990.rss2"},

    # ── NIEDERLANDE (✅ funktioniert laut Log) ────────────────────────────────
    {"land": "Niederlande",          "country": "Niederlande", "url": "https://rss.politie.nl/rss/algemeen/nb/alle-nieuwsberichten.xml"},
    {"land": "Niederlande Opsporing","country": "Niederlande", "url": "https://rss.politie.nl/rss/uitgelicht/nb/alle-uitgelichte-nieuwsberichten.xml"},

    # ── SCHWEIZ (verify_ssl=False wegen Hostname-Mismatch) ───────────────────
    {"land": "Zürich",               "country": "Schweiz",     "url": "https://kapo.zh.ch/internet/sicherheitsdirektion/kapo/de/medienmitteilungen.rss.html", "verify_ssl": False},
    {"land": "Schweiz",              "country": "Schweiz",     "url": "https://www.fedpol.admin.ch/fedpol/de/home/aktuell/medieninformationen.rss.html", "verify_ssl": False},
    {"land": "St. Gallen",           "country": "Schweiz",     "url": "https://www.kapo.sg.ch/home/medienmitteilungen.rss.html", "verify_ssl": False},

    # ── GROSSBRITANNIEN ───────────────────────────────────────────────────────
    # West Yorkshire: ✅ funktioniert laut Log (10 Items)
    {"land": "West Yorkshire",       "country": "Vereinigtes Königreich", "url": "https://www.westyorkshire.police.uk/rss.xml"},
    # Met Police: korrigierte URL
    {"land": "Metropolitan Police",  "country": "Vereinigtes Königreich", "url": "https://news.met.police.uk/feed/"},
    # Hampshire, Merseyside: funktionieren
    {"land": "Hampshire Police",     "country": "Vereinigtes Königreich", "url": "https://www.hampshire.police.uk/news/rss.xml"},
    {"land": "Merseyside Police",    "country": "Vereinigtes Königreich", "url": "https://www.merseyside.police.uk/rss/"},
    {"land": "South Yorkshire",      "country": "Vereinigtes Königreich", "url": "https://www.southyorks.police.uk/rss.xml"},

    # ── FRANKREICH ───────────────────────────────────────────────────────────
    {"land": "France",               "country": "Frankreich",  "url": "https://www.interieur.gouv.fr/rss.xml"},
    {"land": "Gendarmerie",          "country": "Frankreich",  "url": "https://www.gendinfo.fr/rss.xml"},

    # ── BELGIEN ───────────────────────────────────────────────────────────────
    {"land": "Belgien",              "country": "Belgien",     "url": "https://www.police.be/rss/fr/communiques"},

    # ── POLEN (✅ offizieller RSS) ────────────────────────────────────────────
    {"land": "Polen National",       "country": "Polen",       "url": "https://www.policja.pl/pol/rss/1,Aktualnosci.xml"},
    {"land": "Polen Warschau",       "country": "Polen",       "url": "https://www.policja.pl/pol/rss/4,Mazowiecka.xml"},

    # ── EUROPOL ───────────────────────────────────────────────────────────────
    {"land": "Europol",              "country": "Europa",      "url": "https://www.europol.europa.eu/rss/newsroom"},
]


# ── GEOCODING PASS ────────────────────────────────────────────────────────────
def geocode_batch(incidents: list[dict]) -> None:
    need = [i for i in incidents if i["lat"] is None and i.get("location")]
    log.info(f"Geocoding: {len(need)} neue Orte …")
    for inc in need:
        lat, lng = geocode(inc["location"], inc["country"])
        inc["lat"] = lat
        inc["lng"] = lng


def deduplicate(incidents: list[dict]) -> list[dict]:
    seen, result = set(), []
    for inc in incidents:
        if inc["id"] not in seen:
            seen.add(inc["id"])
            result.append(inc)
    return result


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    load_geo_cache()

    existing = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            log.info(f"Bestehend: {len(existing)} Incidents")
        except Exception:
            pass
    existing_ids = {e["id"] for e in existing}

    all_new = []
    ok_count = 0

    # RSS-Quellen
    for src in SOURCES_RSS:
        log.info(f"RSS → {src['land']} ({src['country']})")
        items = parse_rss(
            url=src["url"], land=src["land"], country=src["country"],
            verify_ssl=src.get("verify_ssl", True)
        )
        new = [i for i in items if i["id"] not in existing_ids]
        if items:
            ok_count += 1
        log.info(f"  → {len(new)} neue")
        all_new.extend(new)
        time.sleep(0.4)

    # Österreich HTML
    log.info("HTML → Österreich")
    austria_items = scrape_austria()
    austria_new = [i for i in austria_items if i["id"] not in existing_ids]
    if austria_items:
        ok_count += 1
    log.info(f"  → {len(austria_new)} neue")
    all_new.extend(austria_new)

    log.info(f"Funktionsfähige Quellen: {ok_count}/{len(SOURCES_RSS)+1}")

    # Geocoding
    if all_new:
        geocode_batch(all_new)

    # Zusammenführen
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 30-Tage-Fenster (wie gewünscht)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    combined = [
        i for i in combined
        if i.get("date") and
        datetime.fromisoformat(i["date"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]

    OUTPUT_FILE.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    geo_count = sum(1 for i in combined if i["lat"])
    countries = len({i.get("country","") for i in combined})
    log.info(f"✓ {len(combined)} Incidents | {geo_count} georef. | {countries} Länder | {ok_count} aktive Quellen")


if __name__ == "__main__":
    main()
