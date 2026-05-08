#!/usr/bin/env python3
"""
Europa Polizei OSINT Monitor – Scraper v5 (Telegram-basiert)
=============================================================
Problem: GitHub Actions IPs werden von Presseportal, polizei.bayern.de
         und fast allen Polizei-Webseiten mit 403 blockiert.
Lösung:  Telegram-Kanäle können nicht geblockt werden und liefern
         präzise Ortsangaben direkt im Meldungstext.

Quellen:
  Deutschland:  Offizielle Telegram-Kanäle aller Landespolizeien
  London:       Met Police Twitter/Telegram + RSS (West Yorkshire funktioniert)
  Paris:        Telegram + interieur.gouv.fr
  Brüssel:      Telegram police.be

Geocoding: Nominatim (OSM) – Orte werden aus Freitext extrahiert
Übersetzung: Claude API (Haiku) für EN/FR/NL → DE
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
log = logging.getLogger("polizei-telegram-v5")

OUTPUT_FILE      = Path("data/incidents.json")
GEO_CACHE_FILE   = Path("data/geo_cache.json")
TRANS_CACHE_FILE = Path("data/translation_cache.json")
OUTPUT_FILE.parent.mkdir(exist_ok=True)

ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
TRANSLATE_COUNTRIES = {"Frankreich", "Belgien", "Vereinigtes Königreich"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "de,en;q=0.9",
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
            params={"q": f"{location}, {country}", "format": "json", "limit": 1,
                    "addressdetails": 1},
            headers={"User-Agent": "PolizeiMonitor/5.0 (github.com/BeerSeb)"},
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

# ── ÜBERSETZUNGS-CACHE ─────────────────────────────────────────────────────────
TRANS_CACHE: dict = {}

def load_trans_cache():
    global TRANS_CACHE
    if TRANS_CACHE_FILE.exists():
        TRANS_CACHE = json.loads(TRANS_CACHE_FILE.read_text(encoding="utf-8"))
        log.info(f"Übersetzungs-Cache: {len(TRANS_CACHE)} Einträge")

def save_trans_cache():
    TRANS_CACHE_FILE.write_text(json.dumps(TRANS_CACHE, ensure_ascii=False, indent=2), encoding="utf-8")

def translate_to_german(text: str) -> str:
    if not text or not ANTHROPIC_API_KEY or len(text.strip()) < 10:
        return text
    key = hashlib.md5(text.encode()).hexdigest()
    if key in TRANS_CACHE:
        return TRANS_CACHE[key]
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 512,
                  "system": "Du bist ein präziser Übersetzer für Polizeimeldungen. Antworte NUR mit der deutschen Übersetzung, ohne Kommentar.",
                  "messages": [{"role": "user", "content": f"Übersetze ins Deutsche:\n\n{text}"}]},
            timeout=30,
        )
        translated = resp.json()["content"][0]["text"].strip()
        TRANS_CACHE[key] = translated
        save_trans_cache()
        time.sleep(0.3)
        return translated
    except Exception as e:
        log.warning(f"Übersetzung fehlgeschlagen: {e}")
        return text

def translate_incident(inc: dict) -> dict:
    if inc.get("country") not in TRANSLATE_COUNTRIES or inc.get("translated"):
        return inc
    inc["title_original"]   = inc.get("title", "")
    inc["summary_original"] = inc.get("summary", "")
    inc["title"]   = translate_to_german(inc.get("title", ""))
    inc["summary"] = translate_to_german(inc.get("summary", "")) if inc.get("summary") else ""
    inc["translated"] = True
    return inc

# ── KATEGORISIERUNG ────────────────────────────────────────────────────────────
KATEGORIEN = {
    "Unfall":           ["unfall","kollision","verunglückt","accident","crash","botsing","percuté","heurté"],
    "Einbruch":         ["einbruch","eingebrochen","cambriolage","burglary","inbraak","effraction"],
    "Diebstahl":        ["diebstahl","gestohlen","vol ","theft","diefstal","volé","stolen","entwendet"],
    "Körperverletzung": ["körperverletzung","schlägerei","agression","assault","mishandeling","violence","frappé","angriff"],
    "Betrug":           ["betrug","phishing","fraude","fraud","oplichting","escroquerie","enkeltrick"],
    "Drogen":           ["drogen","drogue","drugs","cannabis","kokain","cocaïne","rauschgift","btm"],
    "Verkehr":          ["trunkenheit","alkohol am steuer","traffic","vitesse","fahrerflucht","trunken","promille"],
    "Vermisstenfall":   ["vermisst","disparition","missing","disparu","gesucht","abgängig"],
    "Brand":            ["brand","feuer","incendie","fire","feu","flammen","rauch","brandstiftung"],
    "Festnahme":        ["festgenommen","verhaftet","arrested","interpellé","arrestation","haft"],
    "Raub":             ["raub","überfall","robbery","braquage","hold-up","räuber","überfallen"],
}

def kategorisieren(text: str) -> str:
    t = text.lower()
    for kat, kws in KATEGORIEN.items():
        if any(kw in t for kw in kws):
            return kat
    return "Sonstiges"

# ── ORTSEXTRAKTION (präzise für Polizeimeldungen) ──────────────────────────────
BLACKLIST = {
    "der","die","das","dem","den","ein","eine","und","oder","aber","für","von",
    "mit","nach","bei","zum","zur","auf","an","am","im","durch","gegen","über",
    "unter","vor","seit","zwischen","beim","polizei","beamte","täter","opfer",
    "donnerstag","freitag","samstag","sonntag","montag","dienstag","mittwoch",
    "januar","februar","märz","april","mai","juni","juli","august","september",
    "oktober","november","dezember","uhr","einsatz","hinweis","ermittlung",
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday",
    "january","february","march","june","july","october","november","december",
}

# Presseportal/Telegram: "Stadt (ots) - Stadtteil - Vorfall"
DASH_RE  = re.compile(r"\(ots\)\s*[-–]\s*([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s/\-][A-ZÄÖÜ][a-zäöüß\-]{2,})?)\s*[-–]", re.U)
# Ereignisort-Label
ERORT_RE = re.compile(r"(?:Ereignisort|Tatort|Einsatzort|Ort der Tat)\s*:?\s*([A-ZÄÖÜ][a-zäöüß\-/\s]{2,40})(?=[,\n\.])", re.U|re.I)
# "Stadt (ots)" am Anfang
OTS_RE   = re.compile(r"^([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s/][A-ZÄÖÜ][a-zäöüß\-]{2,})?)\s*\(ots\)", re.U)
# Zeilenanfang: "Stadtteil - Vorfall"
LEAD_RE  = re.compile(r"^([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s/][A-ZÄÖÜ][a-zäöüß\-]{2,})?)\s*[-–]", re.U|re.M)
# "in Stadtteil," / "in Stadtname."
IN_RE    = re.compile(r"\bin\s+([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[\s\-][A-ZÄÖÜ][a-zäöüß\-]{2,})?)(?=[\s,\.\(])", re.U)

def extrahiere_ort(text: str, fallback: str = "") -> str:
    if not text:
        return fallback
    def ok(s):
        s = s.strip().rstrip(".,;:-")
        return s if (3 <= len(s) <= 60 and s.lower() not in BLACKLIST) else None

    for pattern in [ERORT_RE, DASH_RE, OTS_RE, LEAD_RE, IN_RE]:
        m = pattern.search(text)
        if m:
            r = ok(m.group(1))
            if r:
                return r
    return fallback

def incident_id(src: str, title: str) -> str:
    return hashlib.md5(f"{src}{title}".encode()).hexdigest()[:12]

def parse_date(raw: str) -> str:
    if not raw:
        return datetime.now(timezone.utc).isoformat()
    for fmt in ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S %Z",
                "%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%SZ",
                "%d.%m.%Y %H:%M","%d.%m.%Y","%Y-%m-%d","%d/%m/%Y"]:
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
        return datetime.fromisoformat(date_str.replace("Z","+00:00")).timestamp() >= cutoff
    except Exception:
        return True


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM SCRAPER
# Liest öffentliche Telegram-Kanäle via t.me/s/KANAL (Web-Preview)
# Keine API nötig – funktioniert auch von GitHub Actions
# ══════════════════════════════════════════════════════════════════════════════

def scrape_telegram(channel: str, land: str, country: str,
                    max_pages: int = 5, location_fallback: str = "") -> list[dict]:
    """
    Scrapt einen öffentlichen Telegram-Kanal via t.me/s/CHANNEL.
    Paginiert rückwärts über max_pages Seiten.
    """
    incidents = []
    base_url = f"https://t.me/s/{channel}"
    url = base_url

    for page in range(max_pages):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                log.warning(f"  Telegram {channel} Seite {page+1}: HTTP {r.status_code}")
                break

            soup = BeautifulSoup(r.content, "html.parser")
            messages = soup.select(".tgme_widget_message")

            if not messages:
                break

            for msg in messages:
                # Text
                text_el = msg.select_one(".tgme_widget_message_text")
                if not text_el:
                    continue
                text = text_el.get_text(separator="\n", strip=True)
                if len(text) < 30:
                    continue

                # Datum
                time_el = msg.select_one(".tgme_widget_message_date time")
                date_raw = time_el.get("datetime", "") if time_el else ""
                date_iso = parse_date(date_raw)

                if not within_30_days(date_iso):
                    continue

                # Message-ID für stable ID
                msg_url = ""
                link_el = msg.select_one(".tgme_widget_message_date")
                if link_el and link_el.get("href"):
                    msg_url = link_el["href"]

                # Titel = erste Zeile
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                title = lines[0][:220] if lines else text[:120]
                summary = text[:700]
                full = text

                ort = extrahiere_ort(full, fallback=location_fallback or land)

                incidents.append({
                    "id":       incident_id(msg_url or channel, title),
                    "title":    title,
                    "summary":  summary,
                    "category": kategorisieren(full),
                    "land":     land,
                    "country":  country,
                    "location": ort,
                    "lat":      None, "lng": None,
                    "date":     date_iso,
                    "url":      msg_url,
                    "source":   f"t.me/{channel}",
                })

            # Paginierung: älteste Message-ID für nächste Seite
            if page < max_pages - 1:
                oldest = messages[0] if messages else None
                if oldest:
                    msg_id = oldest.get("data-post", "")
                    if msg_id and "/" in msg_id:
                        num = msg_id.split("/")[-1]
                        url = f"{base_url}?before={num}"
                    else:
                        break
                else:
                    break

            time.sleep(1.0)  # Telegram Rate-Limit

        except Exception as e:
            log.error(f"  Telegram {channel} Seite {page+1}: {e}")
            break

    log.info(f"  Telegram @{channel}: {len(incidents)} Meldungen")
    return incidents


# ══════════════════════════════════════════════════════════════════════════════
# RSS SCRAPER (für Quellen die von GitHub Actions erreichbar sind)
# ══════════════════════════════════════════════════════════════════════════════

def parse_rss(url: str, land: str, country: str,
              max_items: int = 100, location_fallback: str = "") -> list[dict]:
    incidents = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "xml")
        items = soup.find_all("item")[:max_items] or soup.find_all("entry")[:max_items]
        log.info(f"  RSS {land}: {len(items)} Items")

        for item in items:
            t_el  = item.find("title")
            title = t_el.get_text(strip=True) if t_el else ""
            l_el  = item.find("link")
            link  = (l_el.get("href") or l_el.get_text(strip=True)) if l_el else ""
            d_el  = item.find("description") or item.find("summary") or item.find("content")
            desc  = BeautifulSoup(d_el.get_text(strip=True), "html.parser").get_text() if d_el else ""
            p_el  = (item.find("pubDate") or item.find("published") or
                     item.find("dc:date") or item.find("updated"))
            pub   = p_el.get_text(strip=True) if p_el else ""

            if not title or len(title) < 4:
                continue

            full = f"{title} {desc}"
            ort  = extrahiere_ort(full, fallback=location_fallback or land)

            incidents.append({
                "id":       incident_id(link, title),
                "title":    title[:220],
                "summary":  desc[:700],
                "category": kategorisieren(full),
                "land":     land,
                "country":  country,
                "location": ort,
                "lat":      None, "lng": None,
                "date":     parse_date(pub),
                "url":      link,
                "source":   url.split("/")[2],
            })
    except Exception as e:
        log.error(f"  RSS {land}: {e}")
    return incidents


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM-KANÄLE
# Alle offiziellen / verifizierten deutschen Polizei-Telegram-Kanäle
# ══════════════════════════════════════════════════════════════════════════════

TELEGRAM_SOURCES = [
    # ── DEUTSCHLAND ──────────────────────────────────────────────────────────
    # Bayern
    {"channel": "PolizeiMuenchen",          "land": "PP München",          "country": "Deutschland"},
    {"channel": "polizei_muenchen",         "land": "PP München",          "country": "Deutschland"},
    {"channel": "polizei_oberbayern",       "land": "PP Oberbayern",       "country": "Deutschland"},
    {"channel": "PolizeiNuernberg",         "land": "PP Mittelfranken",    "country": "Deutschland"},
    {"channel": "polizei_mittelfranken",    "land": "PP Mittelfranken",    "country": "Deutschland"},
    {"channel": "polizei_unterfranken",     "land": "PP Unterfranken",     "country": "Deutschland"},
    {"channel": "polizei_oberfranken",      "land": "PP Oberfranken",      "country": "Deutschland"},
    {"channel": "polizei_schwaben",         "land": "PP Schwaben",         "country": "Deutschland"},

    # Berlin
    {"channel": "PolizeiBerlin",            "land": "Berlin",              "country": "Deutschland"},
    {"channel": "polizei_berlin_news",      "land": "Berlin",              "country": "Deutschland"},

    # NRW
    {"channel": "polizei_nrw",             "land": "NRW",                  "country": "Deutschland"},
    {"channel": "PolizeiKoeln",            "land": "Köln",                 "country": "Deutschland"},
    {"channel": "polizei_duesseldorf",     "land": "Düsseldorf",           "country": "Deutschland"},
    {"channel": "polizei_dortmund",        "land": "Dortmund",             "country": "Deutschland"},

    # Hamburg
    {"channel": "polizeihamburg",          "land": "Hamburg",              "country": "Deutschland"},

    # Baden-Württemberg
    {"channel": "polizei_bw",             "land": "Baden-Württemberg",    "country": "Deutschland"},
    {"channel": "PolizeiStuttgart",        "land": "Stuttgart",            "country": "Deutschland"},

    # Hessen
    {"channel": "polizei_hessen",          "land": "Hessen",              "country": "Deutschland"},
    {"channel": "PolizeiFFM",              "land": "Frankfurt",            "country": "Deutschland"},

    # Niedersachsen
    {"channel": "polizei_niedersachsen",   "land": "Niedersachsen",       "country": "Deutschland"},
    {"channel": "polizei_hannover",        "land": "Hannover",             "country": "Deutschland"},

    # Sachsen
    {"channel": "polizei_sachsen",         "land": "Sachsen",             "country": "Deutschland"},
    {"channel": "PolizeiDresden",          "land": "Dresden",             "country": "Deutschland"},

    # Andere Bundesländer
    {"channel": "polizei_thueringen",      "land": "Thüringen",           "country": "Deutschland"},
    {"channel": "polizei_sachsenanhalt",   "land": "Sachsen-Anhalt",      "country": "Deutschland"},
    {"channel": "polizei_brandenburgDE",   "land": "Brandenburg",         "country": "Deutschland"},
    {"channel": "polizei_mv",              "land": "Mecklenburg-Vorp.",   "country": "Deutschland"},
    {"channel": "polizei_saarland",        "land": "Saarland",            "country": "Deutschland"},
    {"channel": "polizei_rp",              "land": "Rheinland-Pfalz",     "country": "Deutschland"},
    {"channel": "polizei_sh",              "land": "Schleswig-Holstein",  "country": "Deutschland"},

    # Bundespolizei
    {"channel": "bundespolizei",           "land": "Bundespolizei",       "country": "Deutschland"},
    {"channel": "BKA_Presse",              "land": "BKA",                 "country": "Deutschland"},

    # ── LONDON ───────────────────────────────────────────────────────────────
    {"channel": "metpoliceuk",             "land": "Metropolitan Police", "country": "Vereinigtes Königreich", "loc": "London"},
    {"channel": "londonpolice",            "land": "London Police",       "country": "Vereinigtes Königreich", "loc": "London"},

    # ── PARIS ────────────────────────────────────────────────────────────────
    {"channel": "PoliceNationale",         "land": "Police Nationale",    "country": "Frankreich",             "loc": "Frankreich"},
    {"channel": "Gendarmerie_Nationale",   "land": "Gendarmerie",         "country": "Frankreich",             "loc": "Frankreich"},
    {"channel": "prefpolice",              "land": "Paris Police",        "country": "Frankreich",             "loc": "Paris"},

    # ── BRÜSSEL / BELGIEN ────────────────────────────────────────────────────
    {"channel": "policefederale",          "land": "Police Fédérale",     "country": "Belgien",                "loc": "Brüssel"},
    {"channel": "policebelgique",          "land": "Police Belgique",     "country": "Belgien",                "loc": "Belgien"},
]

# ══════════════════════════════════════════════════════════════════════════════
# RSS-QUELLEN (nur solche die von GitHub Actions erreichbar sind)
# ══════════════════════════════════════════════════════════════════════════════

RSS_SOURCES = [
    # Niederlande (✅ funktioniert laut Log)
    {"land": "Niederlande",     "country": "Niederlande",
     "url": "https://rss.politie.nl/rss/algemeen/nb/alle-nieuwsberichten.xml"},
    {"land": "NL Opsporing",    "country": "Niederlande",
     "url": "https://rss.politie.nl/rss/uitgelicht/nb/alle-uitgelichte-nieuwsberichten.xml"},

    # West Yorkshire (✅ funktioniert laut Log)
    {"land": "West Yorkshire",  "country": "Vereinigtes Königreich",
     "url": "https://www.westyorkshire.police.uk/rss.xml", "loc": "Yorkshire"},

    # Polen (offizieller RSS)
    {"land": "Polen National",  "country": "Polen",
     "url": "https://www.policja.pl/pol/rss/1,Aktualnosci.xml"},

    # Europol
    {"land": "Europol",         "country": "Europa",
     "url": "https://www.europol.europa.eu/rss/newsroom"},
]


# ── GEOCODING BATCH ────────────────────────────────────────────────────────────
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
            raw = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            existing = [e for e in raw if within_30_days(e.get("date", ""))]
            log.info(f"Bestehend (30d): {len(existing)}/{len(raw)}")
        except Exception:
            pass
    existing_ids = {e["id"] for e in existing}

    all_new = []
    ok_telegram = 0
    ok_rss = 0

    # ── TELEGRAM ──────────────────────────────────────────────────────────────
    log.info("=== TELEGRAM SCRAPING ===")
    for src in TELEGRAM_SOURCES:
        channel = src["channel"]
        log.info(f"Telegram @{channel} ({src['land']})")
        items = scrape_telegram(
            channel=channel,
            land=src["land"],
            country=src["country"],
            max_pages=5,
            location_fallback=src.get("loc", ""),
        )
        new = [i for i in items if i["id"] not in existing_ids]
        if items:
            ok_telegram += 1
        all_new.extend(new)
        time.sleep(0.5)

    # ── RSS ───────────────────────────────────────────────────────────────────
    log.info("=== RSS SCRAPING ===")
    for src in RSS_SOURCES:
        log.info(f"RSS {src['land']}")
        items = parse_rss(
            url=src["url"], land=src["land"], country=src["country"],
            max_items=200, location_fallback=src.get("loc", ""),
        )
        new = [i for i in items
               if i["id"] not in existing_ids and within_30_days(i.get("date", ""))]
        if items:
            ok_rss += 1
        all_new.extend(new)
        time.sleep(0.5)

    log.info(f"Telegram-Kanäle aktiv: {ok_telegram}/{len(TELEGRAM_SOURCES)}")
    log.info(f"RSS-Feeds aktiv: {ok_rss}/{len(RSS_SOURCES)}")
    log.info(f"Neue Incidents gesamt: {len(all_new)}")

    # Geocoding
    if all_new:
        geocode_batch(all_new)

    # Übersetzung
    if ANTHROPIC_API_KEY:
        to_translate = [i for i in all_new
                        if i.get("country") in TRANSLATE_COUNTRIES and not i.get("translated")]
        log.info(f"Übersetze {len(to_translate)} internationale Meldungen …")
        for inc in to_translate:
            translate_incident(inc)

    # Zusammenführen + 30-Tage-Fenster
    combined = deduplicate(existing + all_new)
    combined.sort(key=lambda x: x.get("date", ""), reverse=True)

    OUTPUT_FILE.write_text(
        json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    geo      = sum(1 for i in combined if i["lat"])
    countries = len({i.get("country","") for i in combined})
    log.info(f"✓ {len(combined)} Incidents | {geo} georef. | {countries} Länder")


if __name__ == "__main__":
    main()
