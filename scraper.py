#!/usr/bin/env python3
"""
Europa Polizei OSINT Monitor – Scraper v4
==========================================
Strategie: Lokale Polizei-RSS direkt, keine Aggregatoren.

Deutschland:
  - polizei.bayern.de: pro Polizeipräsidium eigener RSS
  - berlin.de/polizei: direkter HTML-Scraper (RSS defekt)
  - Alle anderen Bundesländer: presseportal.de/blaulicht/r/LAND.rss2
    (einzige verlässliche Quelle für BW, NRW, Hessen etc.)

International (Fokus):
  - London: news.met.police.uk (Mynewsdesk-RSS)
  - Paris: prefecturedepolice + interieur.gouv.fr
  - Brüssel: police.be + lokale Zonen

Außerdem:
  - Niederlande: rss.politie.nl (✅ funktioniert)
  - Österreich: polizei.gv.at HTML-Scraping
"""

import json, time, re, hashlib, logging, os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("polizei-scraper-v4")

OUTPUT_FILE        = Path("data/incidents.json")
GEO_CACHE_FILE     = Path("data/geo_cache.json")
TRANS_CACHE_FILE   = Path("data/translation_cache.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

# ── ÜBERSETZUNGS-CACHE + CLAUDE API ───────────────────────────────────────────
TRANS_CACHE: dict = {}
# Länder deren Inhalte übersetzt werden sollen
TRANSLATE_COUNTRIES = {"Frankreich", "Belgien", "Vereinigtes Königreich"}
# Anthropic API Key aus GitHub Secret
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

def load_trans_cache():
    global TRANS_CACHE
    if TRANS_CACHE_FILE.exists():
        TRANS_CACHE = json.loads(TRANS_CACHE_FILE.read_text(encoding="utf-8"))
        log.info(f"Übersetzungs-Cache: {len(TRANS_CACHE)} Einträge")

def save_trans_cache():
    TRANS_CACHE_FILE.write_text(
        json.dumps(TRANS_CACHE, ensure_ascii=False, indent=2), encoding="utf-8"
    )

def translate_to_german(text: str, source_lang: str = "auto") -> str:
    """
    Übersetzt einen Text ins Deutsche via Claude API (claude-haiku-4-5 – schnell & günstig).
    Ergebnis wird gecacht um API-Kosten zu minimieren.
    Ohne API-Key wird der Originaltext zurückgegeben.
    """
    if not text or not text.strip():
        return text
    if not ANTHROPIC_API_KEY:
        return text  # kein Key → kein Übersetzen

    # Cache-Key: MD5 des Originaltexts
    cache_key = hashlib.md5(text.encode()).hexdigest()
    if cache_key in TRANS_CACHE:
        return TRANS_CACHE[cache_key]

    # Zu kurze oder bereits deutsche Texte überspringen
    if len(text.strip()) < 10:
        return text

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 512,
                "messages": [{
                    "role": "user",
                    "content": (
                        f"Übersetze den folgenden Polizeimeldungs-Text präzise ins Deutsche. "
                        f"Gib NUR die Übersetzung zurück, ohne Erklärung oder Kommentar.\n\n{text}"
                    )
                }],
                "system": (
                    "Du bist ein präziser Übersetzer für Polizeimeldungen. "
                    "Übersetze exakt und sachlich ins Deutsche. "
                    "Antworte ausschließlich mit dem übersetzten Text."
                ),
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        translated = data["content"][0]["text"].strip()

        # Im Cache speichern
        TRANS_CACHE[cache_key] = translated
        save_trans_cache()
        time.sleep(0.3)  # Rate-Limit-Puffer
        return translated

    except Exception as e:
        log.warning(f"Übersetzung fehlgeschlagen: {e}")
        return text  # Fallback: Original


def translate_incident(inc: dict) -> dict:
    """Übersetzt Titel und Summary eines Vorfalls wenn nötig."""
    if inc.get("country") not in TRANSLATE_COUNTRIES:
        return inc
    if inc.get("translated"):
        return inc  # bereits übersetzt

    original_title   = inc.get("title", "")
    original_summary = inc.get("summary", "")

    inc["title_original"]   = original_title
    inc["summary_original"] = original_summary
    inc["title"]   = translate_to_german(original_title)
    inc["summary"] = translate_to_german(original_summary) if original_summary else ""
    inc["translated"] = True
    return inc

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PolizeiMonitor/4.0; +https://github.com/BeerSeb)",
    "Accept": "application/rss+xml, application/xml, text/xml, text/html, */*",
    "Accept-Language": "de,en;q=0.9,fr;q=0.8",
}

# ── GEO CACHE ──────────────────────────────────────────────────────────────────
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
            headers={"User-Agent": "PolizeiMonitor/4.0"},
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


# ── KATEGORISIERUNG ────────────────────────────────────────────────────────────
KATEGORIEN = {
    "Unfall":           ["unfall","kollision","verunglückt","accident","crash","botsing","heurté","percuté"],
    "Einbruch":         ["einbruch","eingebrochen","cambriolage","burglary","inbraak","effraction","cambriolé"],
    "Diebstahl":        ["diebstahl","gestohlen","vol ","theft","diefstal","volé","voleur","stolen"],
    "Körperverletzung": ["körperverletzung","schlägerei","agression","assault","mishandeling","violence","aggressione","frappé"],
    "Betrug":           ["betrug","phishing","fraude","fraud","oplichting","arnaque","escroquerie"],
    "Drogen":           ["drogen","drogue","drugs","narkotyki","cannabis","kokain","cocaïne","héroïne","narcotique"],
    "Verkehr":          ["trunkenheit","alkohol am steuer","tráfico","traffic","rijden onder","vitesse","excès de"],
    "Vermisstenfall":   ["vermisst","disparition","missing","vermist","disparu","disparue"],
    "Brand":            ["brand","feuer","incendie","fire","pożar","feu","incendié","arson"],
    "Festnahme":        ["festgenommen","verhaftet","arrested","aangehouden","interpellé","arrêté","arrestation"],
    "Raub":             ["raub","überfall","robbery","braquage","hold-up","overval","braqué"],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIEN.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"

# Ortsextraktion – mehrsprachig
ORT_RE = re.compile(
    r"\b(?:in|bei|aus|in der|à|au|en|at|near|in)\s+"
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
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
        "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d",
        "%d/%m/%Y %H:%M", "%d/%m/%Y",
    ]:
        try:
            return datetime.strptime(raw.strip(), fmt).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ── RSS PARSER ─────────────────────────────────────────────────────────────────
def parse_rss(url: str, land: str, country: str,
              max_items: int = 50, verify_ssl: bool = True,
              location_override: str = "") -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20,
                        verify=verify_ssl, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "xml")
        items = soup.find_all("item")[:max_items] or soup.find_all("entry")[:max_items]
        log.info(f"  ✓ {land}: {len(items)} Items")

        for item in items:
            title = (item.find("title") or object()).__class__.__name__ and ""
            t_el  = item.find("title")
            title = t_el.get_text(strip=True) if t_el else ""
            l_el  = item.find("link")
            link  = (l_el.get("href") or l_el.get_text(strip=True)) if l_el else ""
            d_el  = item.find("description") or item.find("summary") or item.find("content")
            desc  = BeautifulSoup(d_el.get_text(strip=True), "html.parser").get_text() if d_el else ""
            p_el  = item.find("pubDate") or item.find("published") or item.find("dc:date") or item.find("updated")
            pub   = p_el.get_text(strip=True) if p_el else ""

            if not title or len(title) < 4:
                continue

            full = f"{title} {desc}"
            ort  = location_override or extrahiere_ort(full) or land

            incidents.append({
                "id":       incident_id(link, title),
                "title":    title[:220],
                "summary":  desc[:700],
                "category": kategorisieren(full),
                "land":     land,
                "country":  country,
                "location": ort,
                "lat": None, "lng": None,
                "date":     parse_date(pub),
                "url":      link,
                "source":   url.split("/")[2],
            })
    except Exception as e:
        log.error(f"  ✗ {land}: {e}")
    return incidents


# ── HTML SCRAPER ───────────────────────────────────────────────────────────────
def scrape_html(url: str, land: str, country: str,
                item_sel: str, title_sel: str, link_sel: str,
                date_sel: str = "", desc_sel: str = "",
                base_url: str = "", max_items: int = 40,
                verify_ssl: bool = True,
                location_override: str = "") -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=verify_ssl)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        items = soup.select(item_sel)[:max_items]
        log.info(f"  ✓ {land} (HTML): {len(items)} Items")

        for item in items:
            t_el  = item.select_one(title_sel)
            title = t_el.get_text(strip=True) if t_el else ""
            l_el  = item.select_one(link_sel)
            href  = l_el.get("href", "") if l_el else ""
            if href and href.startswith("/"):
                href = base_url + href
            d_el  = item.select_one(date_sel) if date_sel else None
            date_raw = d_el.get_text(strip=True) if d_el else ""
            desc_el  = item.select_one(desc_sel) if desc_sel else None
            desc     = desc_el.get_text(strip=True) if desc_el else ""

            if not title or len(title) < 4:
                continue

            full = f"{title} {desc}"
            ort  = location_override or extrahiere_ort(full) or land

            incidents.append({
                "id":       incident_id(href, title),
                "title":    title[:220],
                "summary":  desc[:700],
                "category": kategorisieren(full),
                "land":     land,
                "country":  country,
                "location": ort,
                "lat": None, "lng": None,
                "date":     parse_date(date_raw),
                "url":      href,
                "source":   url.split("/")[2],
            })
    except Exception as e:
        log.error(f"  ✗ {land} (HTML): {e}")
    return incidents


# ── BERLIN: direkt scrapen ─────────────────────────────────────────────────────
def scrape_berlin() -> list[dict]:
    """berlin.de/polizei: RSS defekt, HTML scrapen."""
    return scrape_html(
        url="https://www.berlin.de/polizei/polizeimeldungen/",
        land="Berlin", country="Deutschland",
        item_sel="article.list--item, .cell.one-whole",
        title_sel="h3.list--headline, .js-link",
        link_sel="a",
        date_sel="span.date, time",
        desc_sel="p.list--abstract",
        base_url="https://www.berlin.de",
        location_override="Berlin",
    )


# ── ÖSTERREICH HTML ────────────────────────────────────────────────────────────
def scrape_austria() -> list[dict]:
    results = []
    endpoints = [
        ("https://www.polizei.gv.at/wien/presse/aussendungen/presse.html",   "Wien"),
        ("https://www.polizei.gv.at/ooe/presse/aussendungen/presse.aspx",    "Oberösterreich"),
        ("https://www.polizei.gv.at/sbg/presse/aussendungen/presse.aspx",    "Salzburg"),
        ("https://www.polizei.gv.at/stmk/presse/aussendungen/presse.aspx",   "Steiermark"),
        ("https://www.polizei.gv.at/tirol/presse/aussendungen/presse.aspx",  "Tirol"),
    ]
    for url, region in endpoints:
        items = scrape_html(
            url=url, land=region, country="Österreich",
            item_sel="tr, .pressemeldung",
            title_sel="td:nth-child(2) a, a",
            link_sel="a",
            date_sel="td:first-child",
            base_url="https://www.polizei.gv.at",
            verify_ssl=False,
            location_override=region,
        )
        results.extend(items)
        time.sleep(0.5)
    return results


# ── PARIS: Préfecture de Police ────────────────────────────────────────────────
def scrape_paris() -> list[dict]:
    results = []

    # 1) interieur.gouv.fr RSS
    results += parse_rss(
        url="https://www.interieur.gouv.fr/actualites/actus-du-ministere.rss",
        land="Paris/Île-de-France", country="Frankreich",
        location_override="Paris",
    )

    # 2) Préfecture de Police HTML (kein funktionierender RSS)
    results += scrape_html(
        url="https://www.prefecturedepolice.interieur.gouv.fr/actualites-et-presse/communiques-de-presse",
        land="Paris Police", country="Frankreich",
        item_sel="article, .communique, .news-item, .item",
        title_sel="h2, h3, .titre, a",
        link_sel="a",
        date_sel="time, .date",
        desc_sel="p, .texte, .resume",
        base_url="https://www.prefecturedepolice.interieur.gouv.fr",
        location_override="Paris",
    )

    # 3) Gendarmerie nationale RSS
    results += parse_rss(
        url="https://www.gendarmerie.interieur.gouv.fr/actualites.rss",
        land="Frankreich Gendarmerie", country="Frankreich",
        location_override="Frankreich",
    )

    log.info(f"  Paris total: {len(results)}")
    return results


# ── BRÜSSEL ────────────────────────────────────────────────────────────────────
def scrape_brussels() -> list[dict]:
    results = []

    # 1) Police Fédérale Belgique
    results += scrape_html(
        url="https://www.police.be/5998/fr/actualites",
        land="Brüssel/Belgien Federal", country="Belgien",
        item_sel="article, .view-content .views-row, .news-item",
        title_sel="h2, h3, .field--name-title, a",
        link_sel="a",
        date_sel=".date, time, .field--name-field-date",
        desc_sel=".field--name-body, p",
        base_url="https://www.police.be",
        location_override="Brüssel",
    )

    # 2) Zone de Police Bruxelles-Capitale/Ixelles
    results += scrape_html(
        url="https://www.policebruxelles.be/fr/actualites",
        land="Brüssel Lokale Polizei", country="Belgien",
        item_sel="article, .news-item, .view-row",
        title_sel="h2, h3, a.title",
        link_sel="a",
        date_sel="time, .date",
        base_url="https://www.policebruxelles.be",
        location_override="Brüssel",
    )

    log.info(f"  Brüssel total: {len(results)}")
    return results


# ── LONDON: Metropolitan Police ───────────────────────────────────────────────
def scrape_london() -> list[dict]:
    results = []

    # Mynewsdesk-RSS der Met Police (aktiv, Stand 2026)
    for tag in ["news", "press_release"]:
        results += parse_rss(
            url=f"https://news.met.police.uk/news_feed.rss?tag={tag}",
            land="London Metropolitan Police", country="Vereinigtes Königreich",
            location_override="London",
        )

    # Fallback: direkte Seite scrapen
    if not results:
        results += scrape_html(
            url="https://news.met.police.uk/latest_news",
            land="London Metropolitan Police", country="Vereinigtes Königreich",
            item_sel=".newsitem, article, .news-list__item",
            title_sel="h2, h3, .newsitem__title",
            link_sel="a",
            date_sel="time, .date, .newsitem__date",
            desc_sel="p, .newsitem__summary",
            base_url="https://news.met.police.uk",
            location_override="London",
        )

    # City of London Police
    results += scrape_html(
        url="https://www.cityoflondon.police.uk/news/city-of-london/news/?newsCategory=Press+releases",
        land="City of London Police", country="Vereinigtes Königreich",
        item_sel="article, .press-release-item, li.news-item",
        title_sel="h2, h3, a",
        link_sel="a",
        date_sel="time, .date",
        base_url="https://www.cityoflondon.police.uk",
        location_override="London",
    )

    log.info(f"  London total: {len(results)}")
    return results


# ── ALLE QUELLEN ───────────────────────────────────────────────────────────────
RSS_SOURCES = [
    # ── DEUTSCHLAND: Bayern direkt (Schema: polizei.bayern.de/PRÄSIDIUM/polizei.rss) ──
    {"land": "PP München",           "country": "Deutschland", "url": "https://www.polizei.bayern.de/muenchen/polizei.rss",        "loc": "München"},
    {"land": "PP Oberbayern Nord",   "country": "Deutschland", "url": "https://www.polizei.bayern.de/oberbayernnord/polizei.rss",  "loc": "Ingolstadt"},
    {"land": "PP Oberbayern Süd",    "country": "Deutschland", "url": "https://www.polizei.bayern.de/oberbayernsued/polizei.rss",  "loc": "Rosenheim"},
    {"land": "PP Mittelfranken",     "country": "Deutschland", "url": "https://www.polizei.bayern.de/mittelfranken/polizei.rss",   "loc": "Nürnberg"},
    {"land": "PP Unterfranken",      "country": "Deutschland", "url": "https://www.polizei.bayern.de/unterfranken/polizei.rss",    "loc": "Würzburg"},
    {"land": "PP Schwaben Nord",     "country": "Deutschland", "url": "https://www.polizei.bayern.de/schwabennord/polizei.rss",    "loc": "Augsburg"},
    {"land": "PP Schwaben Süd/West", "country": "Deutschland", "url": "https://www.polizei.bayern.de/schwabensuedwest/polizei.rss","loc": "Kempten"},
    {"land": "PP Oberfranken",       "country": "Deutschland", "url": "https://www.polizei.bayern.de/oberfranken/polizei.rss",     "loc": "Bayreuth"},
    {"land": "PP Oberpfalz",         "country": "Deutschland", "url": "https://www.polizei.bayern.de/oberpfalz/polizei.rss",       "loc": "Regensburg"},
    {"land": "PP Niederbayern",      "country": "Deutschland", "url": "https://www.polizei.bayern.de/niederbayern/polizei.rss",    "loc": "Landshut"},

    # ── DEUTSCHLAND: Restliche Bundesländer via Presseportal (einzig verlässlicher Weg) ──
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
    {"land": "Mecklenburg-Vorp.",    "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Mecklenburg-Vorpommern.rss2"},
    {"land": "Bremen",               "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/r/Bremen.rss2"},
    # Bundespolizei Presseportal
    {"land": "Bundespolizei",        "country": "Deutschland", "url": "https://www.presseportal.de/blaulicht/nr/73990.rss2"},

    # ── NIEDERLANDE (✅ funktioniert) ─────────────────────────────────────────
    {"land": "Niederlande",          "country": "Niederlande", "url": "https://rss.politie.nl/rss/algemeen/nb/alle-nieuwsberichten.xml"},
    {"land": "NL Opsporing",         "country": "Niederlande", "url": "https://rss.politie.nl/rss/uitgelicht/nb/alle-uitgelichte-nieuwsberichten.xml"},
]


# ── GEOCODING ──────────────────────────────────────────────────────────────────
def geocode_batch(incidents: list[dict]) -> None:
    need = [i for i in incidents if i["lat"] is None and i.get("location")]
    log.info(f"Geocoding: {len(need)} Orte …")
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


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    load_geo_cache()
    load_trans_cache()

    existing = []
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            log.info(f"Bestehend: {len(existing)} Incidents")
        except Exception:
            pass
    existing_ids = {e["id"] for e in existing}

    all_new = []
    ok = 0

    # RSS-Quellen
    for src in RSS_SOURCES:
        log.info(f"RSS → {src['land']}")
        items = parse_rss(
            url=src["url"], land=src["land"], country=src["country"],
            location_override=src.get("loc", ""),
            verify_ssl=src.get("verify_ssl", True),
        )
        new = [i for i in items if i["id"] not in existing_ids]
        if items: ok += 1
        log.info(f"  → {len(new)} neue")
        all_new.extend(new)
        time.sleep(0.35)

    # Berlin direkt
    log.info("HTML → Berlin")
    for i in (x for x in scrape_berlin() if x["id"] not in existing_ids):
        all_new.append(i)
    ok += 1

    # Österreich
    log.info("HTML → Österreich")
    for i in (x for x in scrape_austria() if x["id"] not in existing_ids):
        all_new.append(i)
    ok += 1

    # Paris
    log.info("→ Paris")
    for i in (x for x in scrape_paris() if x["id"] not in existing_ids):
        all_new.append(i)
    ok += 1

    # Brüssel
    log.info("→ Brüssel")
    for i in (x for x in scrape_brussels() if x["id"] not in existing_ids):
        all_new.append(i)
    ok += 1

    # London
    log.info("→ London")
    for i in (x for x in scrape_london() if x["id"] not in existing_ids):
        all_new.append(i)
    ok += 1

    log.info(f"Quellen OK: {ok} | Neue Incidents: {len(all_new)}")

    # Geocoding nur für neue
    if all_new:
        geocode_batch(all_new)

    # Übersetzung für neue internationale Vorfälle
    if ANTHROPIC_API_KEY:
        to_translate = [i for i in all_new if i.get("country") in TRANSLATE_COUNTRIES and not i.get("translated")]
        log.info(f"Übersetze {len(to_translate)} neue internationale Meldungen …")
        for inc in to_translate:
            translate_incident(inc)
    else:
        log.warning("ANTHROPIC_API_KEY nicht gesetzt – Übersetzung übersprungen")

    # Zusammenführen + 30-Tage-Fenster
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    combined = [
        i for i in combined
        if i.get("date") and
        datetime.fromisoformat(i["date"].replace("Z", "+00:00")).timestamp() >= cutoff
    ]

    OUTPUT_FILE.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    geo = sum(1 for i in combined if i["lat"])
    countries = len({i.get("country","") for i in combined})
    log.info(f"✓ {len(combined)} Incidents | {geo} georef. | {countries} Länder | {ok} Quellen")


if __name__ == "__main__":
    main()
