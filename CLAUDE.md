# MediaButler

Self-hosted Web-Tool in Go (stdlib + regexp2), das Downloads in Filme, Serien, Anime und Dokus sortiert und auf ein NFS-Share kopiert. Alle UI-Texte und die Kommunikation sind Deutsch.

## Betrieb
- `docker compose up -d --build` (Image enthält ffmpeg und 7zz).
- Pfade: Quelle `/mnt/share/media/downloads` -> `/incoming`, Ziel `/mnt/share/media` -> `/media`, Einstellungen in `./data` -> `/data`.
- Umgebungsvariablen: SOURCE_DIR, TARGET_DIR, DATA_DIR, PUBLIC_URL, AUTH_USER/AUTH_PASS, API_KEY, MIN_AGE, WATCH_INTERVAL, MOVE_AFTER.
- Update: auf `main` pushen (Workflow baut das Release), dann Update-Knopf in den Einstellungen.

## Aufbau
- `main.go` Routen, Auth, eingebettete UI (`web/`); `handlers.go` API; `engine.go` Scan, Warteschlange, Kopier-Job, Aufräumen.
- `names.go` Erkennung/Benennung (Regexe mit regexp2 wegen Lookaround), Test gegen `testdata/golden.json`.
- `store.go` JSON-Persistenz in DATA_DIR; `cron.go` Zeitplan; `extract.go` 7-Zip; `dupes.go` Duplikate, Abgleich, ffprobe-Analyse; `notify.go` Telegram, Wächter, Jellyfin; `meta.go` Metadaten-Suche; `update.go` Selbst-Update.
- Oberfläche: `web/page.html` (Sidebar am Desktop, Icon-Leiste am Handy).
- Build ohne Netz: `go mod vendor` ist eingecheckt; lokal `GOFLAGS=-mod=mod`.
