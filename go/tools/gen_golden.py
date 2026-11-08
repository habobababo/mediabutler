#!/usr/bin/env python3
"""Erzeugt testdata/golden.json aus der Python-Version: Eingabepfade -> erwartete Zielpfade, Erkennung, Medien-ID."""
import importlib.util, json, os, tempfile
from pathlib import Path
root = Path(__file__).resolve().parents[2]
d = tempfile.mkdtemp(); os.environ.update(DATA_DIR=d, SOURCE_DIR=d, TARGET_DIR=d)
spec = importlib.util.spec_from_file_location("app", root / "app.py"); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
names = """Blade.Runner.2049.2017.german.dl.dv.hdr.2160p.web.h265-GRP.mkv
Blade.Runner.2049.2017.German.DL.1080p.BluRay.x264-GRP/movie.mkv
Dune.Part.Two.2024.GERMAN.DL.DV.HDR.2160p.WEB.H265-WAYNE/dune.part.two.mkv
Dune.Part.Two.2024.GERMAN.DL.DV.HDR.2160p.WEB.H265-WAYNE/wayne-dune.mkv
Breaking.Bad.S01E01.german.dl.1080p.web.h264-GRP.mkv
Breaking Bad (2008)/Breaking.Bad.S01E02.German.720p.mkv
Breaking Bad (2008)/S02E03.mkv
Show.2019.German.1080p/S01E01.mkv
Show.2019.S02E05.GERMAN.DL.DV.HDR.2160p.WEB.H265-GRP.mkv
Show.2019.S02E05.GERMAN.DL.2160p.WEB.H265-GRP.de.forced.srt
Show.2019.S02E05.GERMAN.DL.2160p.WEB.H265-GRP.deu.srt
[SubGroup] Naruto - 05 [1080p][ABCD1234].mkv
[SubGroup] Naruto S2 - 12v2 [720p].mkv
Naruto Shippuden - 023.mkv
Nur.Titel.mkv
Matrix.1999.mkv
The.Matrix.1999.1080p.BluRay.x264.mkv
Heat.1995.mkv
Alien.mkv
Filme/Alien.1979.mkv
Serie.Name.2x05.mkv
Serie.Name.S01E01E02.mkv
Serie.Name.S01E01-E02.mkv
Serie Name Staffel 2 Folge 7.mkv
Serie Name Folge 12.mkv
Serie/Staffel 3/Serie Name 004.mkv
Serie/Season 2/04.mkv
Serie Name/Season 01/Serie.Name.S01E01.mkv
Dokus/Terra X - Die Geschichte 2019.mkv
Planet.Earth.II.2016.2160p.UHD.BluRay.x265.mkv
Der.Herr.der.Ringe.Die.Gefaehrten.2001.EXTENDED.German.DL.1080p.mkv
Spider-Man.No.Way.Home.2021.German.AC3.DL.1080p.mkv
Film (2010) CD1.mkv
Film (2010) CD2.mkv
Film [1999] 720p.mkv
Film {tmdbid-123} (2001).mkv
Movie.Name.2020.2160p.WEB-DL.DDP5.1.Atmos.HEVC-GRP.mkv
Movie.Name.German.2020.1080p.mkv
Tom.und.Jerry.1940.mkv
Ä.Ö.Ü.Test.2005.mkv
Die.Simpsons.S33E01.German.DL.1080p.WEB.h264-GRP.mkv
The.Mandalorian.S02E08.German.DL.2160p.UHD.WEB.HDR.HEVC-GRP.mkv
1883.S01E01.German.1080p.mkv
2012.2009.German.1080p.mkv
Bleach - 366.mkv
One Piece - 1043 [1080p].mkv
Wednesday.2022.S01E05.German.DL.1080p.WEB.h264-GRP.mkv
Folder/Another Folder/Some Movie 2015.mkv
Doku.Name.2018.German.1080p.mkv
.hidden.mkv
Film.mit.Punkt.2011.sub.srt
Show.Name.S03E10.1080p.ger.srt
Show.Name.S03E10.1080p.eng.sdh.srt
Show.Name.S03E10.1080p.srt""".split("\n")
cats = ["Filme", "Serien", "Anime", "Dokus"]
out = []
for rel in names:
    for cat in cats:
        e = m.parse_ep(rel, cat == "Anime", True)
        mid = m.media_id(("Cat/" if cat else "") + rel, 1)
        extra = mid[3]
        if isinstance(extra, tuple): extra = f"S{extra[0]}:" + ",".join(str(x) for x in extra[1])
        else: extra = "" if extra is None else str(extra)
        out.append({"rel": rel, "cat": cat, "target": m.jf_target(rel, cat).as_posix() if hasattr(m.jf_target(rel, cat), "as_posix") else str(m.jf_target(rel, cat)),
                    "legacy": m.legacy_target(rel, cat).as_posix(),
                    "ep": [e[0], e[1], e[2], e[3]] if e else None,
                    "mid": [mid[0], mid[1], mid[2], extra, mid[4]]})
# Kategorie-Erkennung
guess = [{"rel": r, "cat": m.guess_category(r)} for r in names]
json.dump({"cases": out, "guess": guess}, open(root / "go" / "testdata" / "golden.json", "w"), ensure_ascii=False, indent=0)
print(len(out), "Fälle")
