#!/bin/sh
# Legt .env mit zufälligem Passwort und API-Key an und startet MediaButler.
set -e
cd "$(dirname "$0")"
rnd() { head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c "$1"; }
if [ ! -f .env ]; then
  cp .env.example .env
  PASS=$(rnd 16); KEY=$(rnd 40)
  sed -i "s|^AUTH_PASS=.*|AUTH_PASS=$PASS|; s|^API_KEY=.*|API_KEY=$KEY|" .env
  echo "Neu angelegt: .env"
  echo "  Benutzer:  admin"
  echo "  Passwort:  $PASS"
  echo "  API-Key:   $KEY   (für Chrome-Plugin und LabGate)"
else
  echo ".env existiert schon, wird nicht überschrieben."
fi
echo "Pfade für Downloads und Medien in .env prüfen (DOWNLOADS_DIR, MEDIA_DIR)."
printf "Jetzt starten? [J/n] "; read a
case "$a" in n|N) exit 0;; esac
docker compose pull && docker compose up -d
echo "Läuft auf http://$(hostname -I 2>/dev/null | cut -d' ' -f1):$(grep ^PORT= .env | cut -d= -f2):"
