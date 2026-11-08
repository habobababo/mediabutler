#!/usr/bin/env python3
"""Zieht Oberfläche (PAGE) und Icons aus ../app.py nach go/web/. Aufruf: python3 go/tools/extract_assets.py"""
import importlib.util, os, sys, tempfile
from pathlib import Path
root = Path(__file__).resolve().parents[2]
d = tempfile.mkdtemp(); os.environ.update(DATA_DIR=d, SOURCE_DIR=d, TARGET_DIR=d)
spec = importlib.util.spec_from_file_location("app", root / "app.py"); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
web = root / "go" / "web"; web.mkdir(parents=True, exist_ok=True)
(web / "page.html").write_text(m.PAGE, encoding="utf-8")
for n, b in (("icon-64.png", m.ICON_64), ("icon-192.png", m.ICON_192), ("icon-512.png", m.ICON_512)): (web / n).write_bytes(b)
import json; (web / "manifest.json").write_text(json.dumps(m.MANIFEST), encoding="utf-8")
print("ok", len(m.PAGE))
