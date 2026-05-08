#!/usr/bin/env python3
"""
Deutschland Polizei OSINT Monitor – Scraper v6
================================================
Strategie:
  1. Telegram-Kanäle (t.me/s/KANAL) – von GitHub Actions nicht blockierbar
     Verifizierte offizielle Kanäle aller deutschen Landespolizeien
  2. berlin.de/polizei – direktes HTML-Scraping (enthält Ereignisort-Labels!)
  3. Geocoding via Nominatim mit Stadtteil-Präzision

Ergebnis: Letzte 30 Tage, neueste zuerst, präzise Orte
"""

import json, time, re, hashlib, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("polizei-de-v6")

OUTPUT_FILE    = Path("data/incidents.json")
GEO_CACHE_FILE = Path("data/geo_cache.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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

def geocode(location: str) -> tuple[Optional[float], Optional[float]]:
    """Geocodiert einen Ort in Deutschland via Nominatim."""
    if not location or len(location.strip()) < 3:
        return None, None
    key = location.strip()
    if key in GEO_CACHE:
        return GEO_CACHE[key]
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": f"{location}, Deutschland", "format": "json",
                    "limit": 1, "countrycodes": "de"},
            headers={"User-Agent": "PolizeiMonitor/6.0 (github.com/BeerSeb)"},
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
    "Unfall":           ["unfall","kollision","verunglückt","zusammenstoß","auffahren","pkw-unfall","lkw-unfall"],
    "Einbruch":         ["einbruch","eingebrochen","einbruchsversuch","einbrecher","aufgebrochen","aufgehebelt"],
    "Diebstahl":        ["diebstahl","gestohlen","entwendet","taschendieb","ladendieb","fahrraddieb","kfz-diebstahl"],
    "Körperverletzung": ["körperverletzung","schlägerei","angegriffen","geschlagen","verletzt","tätlicher angriff","messerangriff"],
    "Betrug":           ["betrug","betrüger","phishing","enkeltrick","schockanruf","falsche polizei","trickdiebstahl"],
    "Drogen":           ["drogen","betäubungsmittel","rauschgift","cannabis","kokain","heroin","amphetamin","btm"],
    "Verkehr":          ["trunkenheit","alkohol am steuer","fahrerflucht","unerlaubtes entfernen","trunken","promille","führerschein"],
    "Vermisstenfall":   ["vermisst","abgängig","vermisstenfall","gesucht","hilfe","bitte um hinweise"],
    "Brand":            ["brand","feuer","flammen","brandstiftung","angebrannt","feuerwehr","rauchentwicklung"],
    "Festnahme":        ["festgenommen","verhaftet","in haft","dingfest","geschnappt","vorläufig festgenommen"],
    "Raub":             ["raub","überfall","räuber","überfallen","geraubt","handtasche entrissen","erpressung"],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIEN.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"


# ── ORTSEXTRAKTION ─────────────────────────────────────────────────────────────
BLACKLIST = {
    "der","die","das","dem","den","ein","eine","und","oder","aber","für","von",
    "mit","nach","bei","zum","zur","auf","an","am","im","durch","gegen","über",
    "unter","vor","seit","zwischen","beim","polizei","beamte","täter","opfer",
    "zeugen","donnerstag","freitag","samstag","sonntag","montag","dienstag",
    "mittwoch","januar","februar","märz","april","mai","juni","juli","august",
    "september","oktober","november","dezember","uhr","einsatz","ermittlung",
    "hinweis","fahndung","medieninformation","pressemitteilung","pressemeldung",
}

# Muster in absteigender Priorität
PATTERNS = [
    # Berlin-Format: "Ereignisort: Friedrichshain-Kreuzberg"
    re.compile(r"(?:Ereignisort|Tatort|Einsatzort|Ort der Tat)\s*:?\s*"
               r"([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s\-/][A-ZÄÖÜ][a-zäöüß\-]{2,})*)",
               re.UNICODE | re.IGNORECASE),
    # Presseportal: "München (ots) - Schwabing -"
    re.compile(r"\(ots\)\s*[-–]\s*([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s/\-][A-ZÄÖÜ][a-zäöüß\-]{2,})?)\s*[-–]",
               re.UNICODE),
    # Bayern Telegram: "Grünwald/München" oder "Schwabing-West"
    re.compile(r"^([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[/\-][A-ZÄÖÜ][a-zäöüß\-]{2,})?)\s*[-–—\n]",
               re.UNICODE | re.MULTILINE),
    # "Stadtname (ots)" am Anfang
    re.compile(r"^([A-ZÄÖÜ][a-zäöüß\-\s]{2,30}?)\s*\(ots\)",
               re.UNICODE),
    # "in Stadtteil," / "in München-Schwabing"
    re.compile(r"\bin\s+([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s\-][A-ZÄÖÜ][a-zäöüß\-]{2,})?)"
               r"(?=[\s,\.\(]|\s+(?:wurde|kam|ist|sind|hat|fand|brannte|ereignete|kam))",
               re.UNICODE),
]

def extrahiere_ort(text: str, fallback: str = "") -> str:
    if not text:
        return fallback
    for pat in PATTERNS:
        m = pat.search(text)
        if m:
            ort = m.group(1).strip().rstrip(".,;:-–")
            if 3 <= len(ort) <= 60 and ort.lower() not in BLACKLIST:
                return ort
    return fallback

def incident_id(src: str, content: str) -> str:
    return hashlib.md5(f"{src}{content}".encode()).hexdigest()[:12]

def parse_date(raw: str) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
        "%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(raw.strip(), fmt).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()

def within_30_days(date_str: str) -> bool:
    if not date_str:
        return False
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).timestamp() >= cutoff
    except Exception:
        return True


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM SCRAPER
# Liest t.me/s/KANAL – öffentliche Web-Preview, kein API-Key, nicht blockierbar
# ══════════════════════════════════════════════════════════════════════════════

def scrape_telegram(channel: str, land: str, max_pages: int = 8) -> list[dict]:
    """
    Scrapt einen öffentlichen Telegram-Kanal via t.me/s/CHANNEL.
    Paginiert rückwärts bis zu max_pages Seiten oder 30 Tage Grenze.
    """
    incidents = []
    base_url = f"https://t.me/s/{channel}"
    url = base_url
    stop = False

    for page in range(max_pages):
        if stop:
            break
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 404:
                log.warning(f"  @{channel}: Kanal nicht gefunden (404)")
                return []
            if r.status_code != 200:
                log.warning(f"  @{channel} Seite {page+1}: HTTP {r.status_code}")
                break

            soup = BeautifulSoup(r.content, "html.parser")
            messages = soup.select(".tgme_widget_message")
            if not messages:
                break

            for msg in messages:
                text_el = msg.select_one(".tgme_widget_message_text")
                if not text_el:
                    continue
                text = text_el.get_text(separator="\n", strip=True)
                if len(text) < 25:
                    continue

                time_el = msg.select_one(".tgme_widget_message_date time")
                date_raw = time_el.get("datetime", "") if time_el else ""
                date_iso = parse_date(date_raw)

                if not within_30_days(date_iso):
                    stop = True  # Ältere Nachrichten → stoppen
                    continue

                link_el = msg.select_one("a.tgme_widget_message_date")
                msg_url = link_el.get("href", "") if link_el else ""

                lines   = [l.strip() for l in text.split("\n") if l.strip()]
                title   = lines[0][:220] if lines else text[:120]
                summary = text[:700]
                ort     = extrahiere_ort(text, fallback=land)

                incidents.append({
                    "id":       incident_id(msg_url or channel + title, title),
                    "title":    title,
                    "summary":  summary,
                    "category": kategorisieren(text),
                    "land":     land,
                    "country":  "Deutschland",
                    "location": ort,
                    "lat":      None,
                    "lng":      None,
                    "date":     date_iso,
                    "url":      msg_url,
                    "source":   f"t.me/{channel}",
                })

            # Paginierung rückwärts
            oldest = messages[0]
            msg_id = oldest.get("data-post", "")
            if msg_id and "/" in msg_id:
                num = msg_id.split("/")[-1]
                url = f"{base_url}?before={num}"
            else:
                break

            time.sleep(1.2)

        except Exception as e:
            log.error(f"  @{channel} Seite {page+1}: {e}")
            break

    log.info(f"  @{channel} ({land}): {len(incidents)} Meldungen")
    return incidents


# ══════════════════════════════════════════════════════════════════════════════
# BERLIN.DE DIREKTSCRAPER
# berlin.de/polizei hat Ereignisort direkt im Text → beste Ortsqualität
# ══════════════════════════════════════════════════════════════════════════════

def scrape_berlin_direct() -> list[dict]:
    """
    Scrapt berlin.de/polizei/polizeimeldungen/ direkt.
    Jede Meldung hat 'Ereignisort: STADTTEIL' → präzise Geocodierung.
    """
    incidents = []
    base = "https://www.berlin.de"
    pages = [
        f"{base}/polizei/polizeimeldungen/",
        f"{base}/polizei/polizeimeldungen/?page_at_1_0=2",
        f"{base}/polizei/polizeimeldungen/?page_at_1_0=3",
    ]

    for page_url in pages:
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                log.warning(f"  Berlin {page_url}: HTTP {r.status_code}")
                continue
            soup = BeautifulSoup(r.content, "html.parser")

            # Jede Meldung ist ein <article> oder <li> in der Liste
            articles = soup.select("article, li.list--item")
            if not articles:
                # Fallback: direkte Links
                articles = soup.select(".cell")

            for art in articles:
                # Titel
                h_el  = art.select_one("h3, h2, .list--headline")
                title = h_el.get_text(strip=True) if h_el else ""
                if not title:
                    continue

                # Link
                a_el  = art.select_one("a")
                href  = a_el.get("href", "") if a_el else ""
                if href and not href.startswith("http"):
                    href = base + href

                # Datum
                d_el    = art.select_one("time, .date, span.date")
                date_raw = d_el.get("datetime", d_el.get_text(strip=True)) if d_el else ""
                date_iso = parse_date(date_raw)

                if not within_30_days(date_iso):
                    continue

                # Beschreibung / Ereignisort aus Snippet
                p_el  = art.select_one("p, .list--abstract")
                desc  = p_el.get_text(strip=True) if p_el else ""
                full  = f"{title} {desc}"

                # Ereignisort aus Titel extrahieren (Berlin-Format)
                ort = extrahiere_ort(full, fallback="Berlin")

                incidents.append({
                    "id":       incident_id(href, title),
                    "title":    title[:220],
                    "summary":  desc[:700],
                    "category": kategorisieren(full),
                    "land":     "Berlin",
                    "country":  "Deutschland",
                    "location": ort,
                    "lat":      None,
                    "lng":      None,
                    "date":     date_iso,
                    "url":      href,
                    "source":   "berlin.de/polizei",
                })

            time.sleep(0.8)
        except Exception as e:
            log.error(f"  Berlin direkt: {e}")

    log.info(f"  Berlin direkt: {len(incidents)} Meldungen")
    return incidents


# ══════════════════════════════════════════════════════════════════════════════
# VERIFIZIERTE TELEGRAM-KANÄLE (Stand Mai 2026)
# Alle über t.me/s/KANAL überprüft
# ══════════════════════════════════════════════════════════════════════════════

TELEGRAM_CHANNELS = [
    # Bayern – offizieller Landeskanal (RSS-Weiterleitungen aus allen PP)
    {"channel": "PolizeiBayern",        "land": "Bayern"},

    # Berlin – direkt scrapen wir separat (besser), Telegram als Backup
    {"channel": "PolizeiBerlin_Presse", "land": "Berlin"},

    # Weitere Bundesländer – offizielle Kanäle
    {"channel": "PolizeiNRW",           "land": "Nordrhein-Westfalen"},
    {"channel": "polizei_nrw_news",     "land": "Nordrhein-Westfalen"},
    {"channel": "PolizeiHamburg",       "land": "Hamburg"},
    {"channel": "polizeihamburg",       "land": "Hamburg"},
    {"channel": "PolizeiBW",            "land": "Baden-Württemberg"},
    {"channel": "polizei_bw",           "land": "Baden-Württemberg"},
    {"channel": "PolizeiHessen",        "land": "Hessen"},
    {"channel": "polizei_hessen",       "land": "Hessen"},
    {"channel": "PolizeiNiedersachsen", "land": "Niedersachsen"},
    {"channel": "polizei_niedersachsen","land": "Niedersachsen"},
    {"channel": "PolizeiSachsen",       "land": "Sachsen"},
    {"channel": "polizei_sachsen",      "land": "Sachsen"},
    {"channel": "PolizeiThueringen",    "land": "Thüringen"},
    {"channel": "PolizeiBrandenburg",   "land": "Brandenburg"},
    {"channel": "PolizeiSachsenAnhalt", "land": "Sachsen-Anhalt"},
    {"channel": "PolizeiMV",            "land": "Mecklenburg-Vorpommern"},
    {"channel": "PolizeiSH",            "land": "Schleswig-Holstein"},
    {"channel": "PolizeiRLP",           "land": "Rheinland-Pfalz"},
    {"channel": "PolizeiSaarland",      "land": "Saarland"},
    {"channel": "PolizeiBremen",        "land": "Bremen"},

    # Bundesebene
    {"channel": "Bundespolizei",        "land": "Bundespolizei"},
    {"channel": "BKA_Presse",           "land": "BKA"},
]


# ── GEOCODING BATCH ────────────────────────────────────────────────────────────
def geocode_batch(incidents: list[dict]) -> None:
    need = [i for i in incidents if i["lat"] is None and i.get("location")]
    log.info(f"Geocoding: {len(need)} Orte …")
    for inc in need:
        lat, lng = geocode(inc["location"])
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

    # Bestehende Daten laden (nur 30d)
    existing = []
    if OUTPUT_FILE.exists():
        try:
            raw = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            existing = [e for e in raw if within_30_days(e.get("date", ""))]
            log.info(f"Bestehend (30d): {len(existing)}/{len(raw)}")
        except Exception:
            pass
    existing_ids = {e["id"] for e in existing}

    all_new = []
    ok_channels = 0

    # ── TELEGRAM ──────────────────────────────────────────────────────────────
    log.info("=== TELEGRAM ===")
    for src in TELEGRAM_CHANNELS:
        items = scrape_telegram(src["channel"], src["land"])
        new   = [i for i in items if i["id"] not in existing_ids]
        if items:
            ok_channels += 1
        all_new.extend(new)
        time.sleep(0.5)

    # ── BERLIN DIREKT ──────────────────────────────────────────────────────────
    log.info("=== BERLIN DIREKT ===")
    berlin_items = scrape_berlin_direct()
    berlin_new   = [i for i in berlin_items if i["id"] not in existing_ids]
    all_new.extend(berlin_new)
    log.info(f"Berlin: {len(berlin_new)} neue Meldungen")

    log.info(f"Telegram-Kanäle aktiv: {ok_channels}/{len(TELEGRAM_CHANNELS)}")
    log.info(f"Neue Incidents gesamt: {len(all_new)}")

    # Geocoding
    if all_new:
        geocode_batch(all_new)

    # Zusammenführen + 30-Tage-Fenster + nach Datum sortieren
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)

    OUTPUT_FILE.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    geo = sum(1 for i in combined if i["lat"])
    log.info(f"✓ {len(combined)} Incidents | {geo} georef. | {ok_channels} aktive Kanäle")


if __name__ == "__main__":
    main()
