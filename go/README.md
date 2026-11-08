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
| Duplikate, Bibliotheks-Abgleich der Neuzugänge, Codec-Analyse | fertig, Gruppen identisch zur Python-Version |
| Telegram (Meldungen, Wächter, Passwort per Antwort), Jellyfin-Scan | fertig |
| Metadaten-Suche (TMDB/TVDB/AniList/Jikan) | fertig, Antworten identisch zur Python-Version |
| Update-Knopf | fertig, lädt Release-Asset `mediabutler-linux-<arch>` (siehe unten) |

## Bauen / Testen

    cd go
    go test ./...
    go build -o mediabutler .
    docker build -t mediabutler-go .      # nutzt vendor/, kein Netz für Go-Module nötig

Die Web-UI liegt in `web/page.html` (aus `app.py` extrahiert via `tools/extract_assets.py`).

## Update-Knopf

Die Go-Version aktualisiert sich nicht aus dem Quelltext, sondern aus einem GitHub-Release:
`git tag v1.2.0 && git push --tags` startet `.github/workflows/go-release.yml` (baut amd64 und arm64, führt die Tests aus).
Der Knopf in den Einstellungen lädt das passende Asset, sichert die alte Datei (`DATA_DIR/mediabutler.bak`), ersetzt das Programm und beendet es;
`restart: unless-stopped` startet den Container neu. Ein neues Image (`docker compose build`) ersetzt solche Updates wieder.
Für ein privates Repo wird wie bisher ein Token mit Lesezugriff auf Contents gebraucht.
