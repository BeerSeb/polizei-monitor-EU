# 🗺️ DE Polizei OSINT Monitor

Interaktiver Heatmap-Monitor für Polizeimeldungen aller 16 deutschen Bundesländer + Bundespolizei.

**Live:** `https://<dein-username>.github.io/polizei-monitor-de`

---

## Features

- **Heatmap** & **Punktkarte** via Leaflet.js (OpenStreetMap)
- **Volltext-Suche** nach Ort, Stichwort oder Bundesland
- **Filter** nach Bundesland, Zeitraum, Kategorie
- **Detail-Panel** mit Zusammenfassung & Link zur Originalmeldung
- **Excel-Export** der gefilterten Vorfälle
- Automatische **Geocodierung** via Nominatim (mit lokalem Cache)
- **Täglich aktualisiert** via GitHub Actions (07:00 UTC)

## Datenquellen

| Quelle | Feed |
|---|---|
| Bundespolizei | bundespolizei.de RSS |
| Bayern | polizei.bayern.de RSS |
| Berlin | berlin.de/polizei RSS |
| Baden-Württemberg | polizei-bw.de RSS |
| Hessen | polizei.hessen.de RSS |
| … alle 16 Bundesländer | Presseportal / Direkt-RSS |

## Setup

### 1. Repo anlegen & GitHub Pages aktivieren

```bash
git init polizei-monitor-de
cd polizei-monitor-de
# Dateien reinkopieren (index.html, scraper.py, .github/)
git add . && git commit -m "init"
git remote add origin https://github.com/<user>/polizei-monitor-de.git
git push -u origin main
```

GitHub → Settings → Pages → Branch: `main` / Root

### 2. Abhängigkeiten (lokal testen)

```bash
pip install requests beautifulsoup4 lxml
mkdir -p data
python scraper.py
```

### 3. GitHub Actions

Der Workflow unter `.github/workflows/scrape.yml` läuft täglich um 07:00 UTC automatisch.  
Manuell starten: Actions → "Scrape DE Polizei Monitor" → "Run workflow"

## Projektstruktur

```
polizei-monitor-de/
├── index.html                  # Frontend (Leaflet, Filter, Tabelle)
├── scraper.py                  # Multi-Source Scraper + Geocoder
├── data/
│   ├── incidents.json          # Generierte Vorfallsdaten
│   └── geo_cache.json          # Nominatim-Cache (spart API-Calls)
└── .github/
    └── workflows/
        └── scrape.yml          # GitHub Actions Workflow
```

## Hinweise

- Nominatim-Rate-Limit: 1 Request/Sekunde – bei vielen neuen Orten kann der erste Lauf etwas dauern
- Der Geo-Cache wird zwischen Runs in `data/geo_cache.json` persistiert
- Vorfälle älter als 90 Tage werden automatisch entfernt
- RSS-Feed-URLs können sich ändern – bei Scraping-Fehlern bitte `scraper.py` prüfen

## Verwandt

- [PP München OSINT Monitor](https://beerseb.github.io/polizei-monitor) – der ursprüngliche Monitor für München
