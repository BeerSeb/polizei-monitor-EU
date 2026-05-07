#!/usr/bin/env python3
"""
Europa Polizei OSINT Monitor – Scraper
========================================
Scrapt Polizeipressemitteilungen aus 11 europäischen Ländern,
geocodiert Ortsangaben via Nominatim und speichert incidents.json.

Länder: DE, AT, CH, NL, FR, BE, ES, IT, GB, CZ, PL

GitHub Actions: täglich 07:00 UTC
"""

import json
import time
import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("europa-scraper")

OUTPUT_FILE    = Path("data/incidents.json")
GEO_CACHE_FILE = Path("data/geo_cache.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

HEADERS = {"User-Agent": "Europa-Polizei-OSINT-Monitor/2.0 (github.com/BeerSeb/polizei-monitor-de)"}

# ── GEO CACHE ────────────────────────────────────────────────────────────────
GEO_CACHE: dict = {}

def load_geo_cache():
    global GEO_CACHE
    if GEO_CACHE_FILE.exists():
        GEO_CACHE = json.loads(GEO_CACHE_FILE.read_text(encoding="utf-8"))
        log.info(f"Geo-Cache: {len(GEO_CACHE)} Einträge")

def save_geo_cache():
    GEO_CACHE_FILE.write_text(json.dumps(GEO_CACHE, ensure_ascii=False, indent=2), encoding="utf-8")

def geocode(location: str, country: str) -> tuple[Optional[float], Optional[float]]:
    if not location or len(location) < 3:
        return None, None
    key = f"{location}|{country}"
    if key in GEO_CACHE:
        return GEO_CACHE[key]
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{location}, {country}", "format": "json", "limit": 1},
            headers=HEADERS, timeout=10,
        )
        time.sleep(1.1)
        data = r.json()
        if data:
            result = (float(data[0]["lat"]), float(data[0]["lon"]))
            GEO_CACHE[key] = result
            save_geo_cache()
            return result
    except Exception as e:
        log.warning(f"Geocoding fehlgeschlagen '{location}': {e}")
    GEO_CACHE[key] = (None, None)
    return None, None


# ── KATEGORISIERUNG ───────────────────────────────────────────────────────────
# Mehrsprachige Keywords
KATEGORIE_KEYWORDS = {
    "Unfall": [
        "unfall","kollision","verunglückt","accident","accidente","incidente",
        "wypadek","nehoda","crash","botsing","aanrijding","haccident",
    ],
    "Einbruch": [
        "einbruch","eingebrochen","cambriolage","robo","burglary","inbraak",
        "wlam","vloupání","rapina","effraction",
    ],
    "Diebstahl": [
        "diebstahl","gestohlen","vol","hurto","theft","diefstal","kradzież",
        "krádež","furto","straat","taschendieb",
    ],
    "Körperverletzung": [
        "körperverletzung","schlägerei","agression","agresión","assault",
        "mishandeling","pobicie","napadení","aggressione","violence",
    ],
    "Betrug": [
        "betrug","phishing","fraude","fraud","oplichting","oszustwo",
        "podvod","truffa","escroquerie",
    ],
    "Drogen": [
        "drogen","drogue","droga","drugs","narkotyki","drogy","droga",
        "cannabis","kokain","heroin","cocaïne",
    ],
    "Verkehr": [
        "trunkenheit","alkohol am steuer","conduite","tráfico","traffic",
        "rijden onder invloed","kierowanie pod wpływem","řízení pod vlivem",
        "guida in stato","vitesse","speeding",
    ],
    "Vermisstenfall": [
        "vermisst","disparition","desaparecido","missing","vermist","zaginięcie",
        "pohřešování","scomparsa",
    ],
    "Brand": [
        "brand","feuer","incendie","incendio","fire","brand","pożar","požár",
        "incendio","arson",
    ],
    "Festnahme": [
        "festgenommen","verhaftet","arrestation","detenido","arrested",
        "aangehouden","zatrzymany","zadržen","arrestato","arrest",
    ],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIE_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"


# ── ORT EXTRAHIEREN ───────────────────────────────────────────────────────────
ORT_PATTERNS = [
    # Deutsch
    re.compile(r"\b(?:in|bei|aus|im Bereich|Gemeinde|Stadt|Kreis)\s+([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:\s[A-ZÄÖÜ][a-zäöüß\-]{2,})?)", re.U),
    # Englisch
    re.compile(r"\b(?:in|at|near|from)\s+([A-Z][a-z]{2,}(?:\s[A-Z][a-z]{2,})?)", re.U),
    # Französisch
    re.compile(r"\b(?:à|au|dans le?|près de)\s+([A-ZÀÂÉÈÊËÎÏÔÙÛÜ][a-zàâéèêëîïôùûü\-]{2,}(?:\s[A-ZÀÂÉÈÊËÎÏÔÙÛÜ][a-zàâéèêëîïôùûü\-]{2,})?)", re.U),
    # Spanisch/Italienisch
    re.compile(r"\b(?:en|en la ciudad de|a|in)\s+([A-ZÁÉÍÓÚÀÈÉÌÒÙ][a-záéíóúàèéìòù\-]{2,}(?:\s[A-ZÁÉÍÓÚÀÈÉÌÒÙ][a-záéíóúàèéìòù\-]{2,})?)", re.U),
    # Niederländisch
    re.compile(r"\b(?:in|te|bij|nabij)\s+([A-Z][a-zéèëï\-]{2,}(?:\s[A-Z][a-zéèëï\-]{2,})?)", re.U),
]

def extrahiere_ort(text: str) -> str:
    for pat in ORT_PATTERNS:
        m = pat.search(text)
        if m:
            ort = m.group(1).strip()
            if len(ort) >= 3:
                return ort
    return ""


def incident_id(url: str, title: str) -> str:
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:12]


def parse_date(raw: str) -> str:
    raw = raw.strip()
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(raw, fmt).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ── RSS PARSER ────────────────────────────────────────────────────────────────
def parse_rss(url: str, land: str, country_name: str, max_items: int = 40) -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
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
            desc  = BeautifulSoup(desc.get_text(strip=True), "html.parser").get_text() if desc else ""
            pub   = item.find("pubDate") or item.find("published") or item.find("dc:date") or item.find("updated")
            pub   = pub.get_text(strip=True) if pub else ""

            if not title:
                continue

            full_text = f"{title} {desc}"
            ort = extrahiere_ort(full_text) or land
            kat = kategorisieren(full_text)
            date_iso = parse_date(pub) if pub else datetime.now(timezone.utc).isoformat()
            domain = url.split("/")[2]

            incidents.append({
                "id":           incident_id(link, title),
                "title":        title[:200],
                "summary":      desc[:600] if desc else "",
                "category":     kat,
                "land":         land,
                "country":      country_name,
                "location":     ort,
                "lat":          None,
                "lng":          None,
                "date":         date_iso,
                "url":          link,
                "source":       domain,
            })
    except Exception as e:
        log.error(f"  Fehler {land} ({url}): {e}")
    return incidents


# ── HTML SCRAPER (für Länder ohne RSS) ────────────────────────────────────────
def scrape_html_list(url: str, land: str, country_name: str,
                      item_selector: str, title_sel: str, link_sel: str,
                      date_sel: str = "", desc_sel: str = "",
                      max_items: int = 30) -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        items = soup.select(item_selector)[:max_items]
        log.info(f"  {land} (HTML): {len(items)} Items")

        for item in items:
            t_el = item.select_one(title_sel)
            title = t_el.get_text(strip=True) if t_el else ""
            l_el = item.select_one(link_sel)
            link = l_el.get("href", "") if l_el else ""
            if link and not link.startswith("http"):
                base = "/".join(url.split("/")[:3])
                link = base + link
            d_el = item.select_one(date_sel) if date_sel else None
            date_raw = d_el.get_text(strip=True) if d_el else ""
            desc_el = item.select_one(desc_sel) if desc_sel else None
            desc = desc_el.get_text(strip=True) if desc_el else ""

            if not title:
                continue

            full_text = f"{title} {desc}"
            ort = extrahiere_ort(full_text) or land
            kat = kategorisieren(full_text)
            date_iso = parse_date(date_raw) if date_raw else datetime.now(timezone.utc).isoformat()

            incidents.append({
                "id":       incident_id(link, title),
                "title":    title[:200],
                "summary":  desc[:600],
                "category": kat,
                "land":     land,
                "country":  country_name,
                "location": ort,
                "lat":      None,
                "lng":      None,
                "date":     date_iso,
                "url":      link,
                "source":   url.split("/")[2],
            })
    except Exception as e:
        log.error(f"  HTML-Scraping Fehler {land} ({url}): {e}")
    return incidents


# ── QUELLEN ───────────────────────────────────────────────────────────────────
# Alle Einträge: {"land", "country", "type": "rss"|"html", "url", ...}

SOURCES_RSS = [
    # ── DEUTSCHLAND ──────────────────────────────────────────────────────────
    {"land": "Bundespolizei",           "country": "Deutschland", "url": "https://www.bundespolizei.de/Web/DE/04Aktuelles/01Meldungen/meldungen_node.html?view=renderRSS"},
    {"land": "Bayern",                  "country": "Deutschland", "url": "https://www.polizei.bayern.de/aktuelles/pressemitteilungen/index.html/rss"},
    {"land": "Berlin",                  "country": "Deutschland", "url": "https://www.berlin.de/polizei/polizeimeldungen/rss.php"},
    {"land": "Baden-Württemberg",       "country": "Deutschland", "url": "https://www.polizei-bw.de/rss-feeds/pressemitteilungen.rss"},
    {"land": "Hessen",                  "country": "Deutschland", "url": "https://www.polizei.hessen.de/presse/Pressemitteilungen/?view=rss"},
    {"land": "NRW",                     "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6335.rss2"},
    {"land": "Niedersachsen",           "country": "Deutschland", "url": "https://www.pd-h.polizei-nds.de/dienststellen/presse_und_oeffentlichkeitsarbeit/rss.xml"},
    {"land": "Brandenburg",             "country": "Deutschland", "url": "https://www.polizei.brandenburg.de/pressemitteilungen/?type=9818"},
    {"land": "Sachsen",                 "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6337.rss2"},
    {"land": "Thüringen",               "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6343.rss2"},
    {"land": "Sachsen-Anhalt",          "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6338.rss2"},
    {"land": "Schleswig-Holstein",      "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6341.rss2"},
    {"land": "Rheinland-Pfalz",         "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6339.rss2"},
    {"land": "Saarland",                "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6334.rss2"},
    {"land": "Bremen",                  "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6337.rss2"},
    {"land": "Mecklenburg-Vorpommern",  "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6200.rss2"},
    {"land": "Hamburg",                 "country": "Deutschland", "url": "https://www.presseportal.de/rss/pid_6347.rss2"},

    # ── ÖSTERREICH ────────────────────────────────────────────────────────────
    {"land": "Wien",                    "country": "Österreich",  "url": "https://www.polizei.gv.at/alle/rss.aspx"},
    {"land": "Österreich",              "country": "Österreich",  "url": "https://www.bmi.gv.at/rss_feed/start.aspx"},

    # ── NIEDERLANDE ───────────────────────────────────────────────────────────
    {"land": "Niederlande",             "country": "Niederlande", "url": "https://rss.politie.nl/rss/algemeen/nb/alle-nieuwsberichten.xml"},
    {"land": "Niederlande Opsporing",   "country": "Niederlande", "url": "https://rss.politie.nl/rss/uitgelicht/nb/alle-uitgelichte-nieuwsberichten.xml"},

    # ── SCHWEIZ ───────────────────────────────────────────────────────────────
    {"land": "Zürich",                  "country": "Schweiz",     "url": "https://www.kapo.zh.ch/internet/sicherheitsdirektion/kapo/de/medienmitteilungen.rss.html"},
    {"land": "Bern",                    "country": "Schweiz",     "url": "https://www.kapo.be.ch/kapo/de/index/aktuell/medienmitteilungen.rss.html"},
    {"land": "Basel",                   "country": "Schweiz",     "url": "https://www.kantonspolizei.bs.ch/medienmitteilungen.rss.html"},
    {"land": "Schweiz-Presseportal",    "country": "Schweiz",     "url": "https://www.presseportal.ch/de/rss/dienststelle/100001216"},

    # ── GROSSBRITANNIEN ───────────────────────────────────────────────────────
    {"land": "Metropolitan Police",     "country": "Vereinigtes Königreich", "url": "https://news.met.police.uk/news.rss"},
    {"land": "West Yorkshire Police",   "country": "Vereinigtes Königreich", "url": "https://www.westyorkshire.police.uk/rss.xml"},
    {"land": "Greater Manchester",      "country": "Vereinigtes Königreich", "url": "https://www.gmp.police.uk/rss/"},
    {"land": "Thames Valley Police",    "country": "Vereinigtes Königreich", "url": "https://www.thamesvalley.police.uk/rss/"},

    # ── FRANKREICH (via Presseportal & interieur.gouv.fr) ────────────────────
    {"land": "France - Intérieur",      "country": "Frankreich",  "url": "https://www.interieur.gouv.fr/rss.xml"},
    {"land": "France - Gendarmerie",    "country": "Frankreich",  "url": "https://www.gendarmerie.interieur.gouv.fr/cegn/rss.xml"},

    # ── BELGIEN ───────────────────────────────────────────────────────────────
    {"land": "Belgien Federal",         "country": "Belgien",     "url": "https://www.police.be/rss/fr/actualites"},
    {"land": "Belgien Lokaal",          "country": "Belgien",     "url": "https://www.localpolice.be/rss/fr/actualites"},

    # ── POLEN ─────────────────────────────────────────────────────────────────
    {"land": "Polska Krajowa",          "country": "Polen",       "url": "https://www.policja.pl/pol/rss/1,Aktualnosci.xml"},
    {"land": "Polska Warszawa",         "country": "Polen",       "url": "https://www.policja.pl/pol/rss/4,Mazowiecka.xml"},

    # ── EUROPOL (übergreifend) ────────────────────────────────────────────────
    {"land": "Europol",                 "country": "Europa",      "url": "https://www.europol.europa.eu/rss/newsroom"},
]

# HTML-Quellen (für Länder ohne verlässlichen RSS)
SOURCES_HTML = [
    # ── SPANIEN ───────────────────────────────────────────────────────────────
    {
        "land": "España Nacional",
        "country": "Spanien",
        "url": "https://www.policia.es/prensa/noticias_listado.html",
        "item_selector": "article.noticia, .noticia-item, li.item",
        "title_sel": "h2, h3, .titulo",
        "link_sel": "a",
        "date_sel": ".fecha, time",
        "desc_sel": ".resumen, p",
    },
    # ── ITALIEN ───────────────────────────────────────────────────────────────
    {
        "land": "Italia Nazionale",
        "country": "Italien",
        "url": "https://www.poliziadistato.it/comunicati-stampa",
        "item_selector": "article, .comunicato, .news-item",
        "title_sel": "h2, h3, .titolo",
        "link_sel": "a",
        "date_sel": ".data, time",
        "desc_sel": ".testo, p",
    },
    # ── TSCHECHIEN ────────────────────────────────────────────────────────────
    {
        "land": "Česká republika",
        "country": "Tschechien",
        "url": "https://www.policie.cz/clanek/tiskove-zpravy.aspx",
        "item_selector": ".article-item, article, li.news",
        "title_sel": "h2, h3, a.title",
        "link_sel": "a",
        "date_sel": ".date, time",
        "desc_sel": "p, .perex",
    },
    # ── FRANKREICH (Fallback HTML) ────────────────────────────────────────────
    {
        "land": "Paris Police HTML",
        "country": "Frankreich",
        "url": "https://www.prefecturedepolice.interieur.gouv.fr/actualites-et-presse/communiques-de-presse/communiques-de-presse",
        "item_selector": "article, .communique-item, .actualite",
        "title_sel": "h2, h3, .titre",
        "link_sel": "a",
        "date_sel": "time, .date",
        "desc_sel": "p, .texte",
    },
]


# ── GEOCODING PASS ────────────────────────────────────────────────────────────
def geocode_batch(incidents: list[dict]) -> list[dict]:
    need = [i for i in incidents if i["lat"] is None and i.get("location")]
    log.info(f"Geocoding: {len(need)} neue Orte …")
    for inc in need:
        lat, lng = geocode(inc["location"], inc["country"])
        inc["lat"] = lat
        inc["lng"] = lng
    return incidents


# ── DEDUPLICATION ─────────────────────────────────────────────────────────────
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

    # Bestehende Daten laden
    existing = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            log.info(f"Bestehend: {len(existing)} Incidents")
        except Exception:
            pass
    existing_ids = {e["id"] for e in existing}

    all_new = []

    # RSS-Quellen
    for src in SOURCES_RSS:
        log.info(f"RSS → {src['land']} ({src['country']})")
        items = parse_rss(src["url"], src["land"], src["country"])
        new = [i for i in items if i["id"] not in existing_ids]
        log.info(f"  → {len(new)} neue")
        all_new.extend(new)
        time.sleep(0.3)

    # HTML-Quellen
    for src in SOURCES_HTML:
        log.info(f"HTML → {src['land']} ({src['country']})")
        items = scrape_html_list(
            url=src["url"], land=src["land"], country_name=src["country"],
            item_selector=src["item_selector"], title_sel=src["title_sel"],
            link_sel=src["link_sel"],
            date_sel=src.get("date_sel", ""), desc_sel=src.get("desc_sel", ""),
        )
        new = [i for i in items if i["id"] not in existing_ids]
        log.info(f"  → {len(new)} neue")
        all_new.extend(new)
        time.sleep(0.5)

    # Geocoding (nur neue)
    if all_new:
        geocode_batch(all_new)

    # Zusammenführen, deduplizieren, sortieren
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)

    # 90-Tage-Fenster
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).timestamp()
    combined = [
        i for i in combined
        if i.get("date") and
        datetime.fromisoformat(i["date"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]

    OUTPUT_FILE.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    geo_count = sum(1 for i in combined if i["lat"])
    countries  = len({i["country"] for i in combined})
    log.info(f"✓ {len(combined)} Incidents | {geo_count} georef. | {countries} Länder")


if __name__ == "__main__":
    main()
