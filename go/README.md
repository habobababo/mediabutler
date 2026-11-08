# MediaButler – Go-Port (Branch `go`)

Gleiche Web-Oberfläche, gleiche JSON-Dateien in `DATA_DIR`, gleiche Umgebungsvariablen wie die Python-Version.
Ein statisches Binary statt Python + Flask.

## Stand

| Bereich | Status |
|---|---|
| Benennung (Jellyfin, Staffeln, Tags, IDs) | fertig, 216 Golden-Tests gegen Python identisch |
| Scan, Kategorien, Gruppen, eigene Zielordner, Regeln | fertig |
| Kopier-Queue, Cancel, Verlauf, Move-after, Leftovers, Samples | fertig |
| Zeitplan (Cron), Einstellungen, Speicherbalken, Auth, Icons | fertig |
| Entpacken (RAR/ZIP/7z, mehrteilig, Passwörter), Hook | fertig (benötigt 7zz) |
| Duplikate, Analyse | folgt (liefert 501) |
| Metadaten (TMDB/TVDB/AniList/Jikan), Telegram, Jellyfin-Scan, Update-Knopf | folgt (liefert 501) |

## Bauen / Testen

    cd go
    go test ./...
    go build -o mediabutler .
    docker build -t mediabutler-go .      # nutzt vendor/, kein Netz für Go-Module nötig

Die Web-UI liegt in `web/page.html` (aus `app.py` extrahiert via `tools/extract_assets.py`).
