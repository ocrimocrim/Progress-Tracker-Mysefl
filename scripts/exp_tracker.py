#!/usr/bin/env python3
import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

RANKING_URL = "https://pr-underworld.com/website/ranking/"
SERVER_HEADING_TEXT = "Netherworld"

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"

CHARACTERS_FILE = CONFIG_DIR / "characters.txt"
DUNGEONS_FILE = CONFIG_DIR / "dungeons.json"

RUNS_CSV = DATA_DIR / "runs.csv"
RESULTS_CSV = DATA_DIR / "results_per_char.csv"
ERRORS_CSV = DATA_DIR / "errors.csv"

HEADERS = {
    "User-Agent": "NetherworldEXPTracker/1.0 (+GitHub Actions)"
}

BERLIN = ZoneInfo("Europe/Berlin")

@dataclass
class CharSnapshot:
    character: str
    level: int
    exp_pct: float  # 27.42 wird 27.42, .08 wird 0.08

def now_berlin() -> datetime:
    return datetime.now(tz=timezone.utc).astimezone(BERLIN)

def format_ts(dt: datetime) -> str:
    return dt.astimezone(BERLIN).strftime("%Y-%m-%d %H Uhr %M")

def run_id_from_start(ts: datetime) -> str:
    return ts.astimezone(BERLIN).strftime("R%Y%m%d_%H%M%S")

def ensure_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if not RUNS_CSV.exists():
        RUNS_CSV.write_text(
            "run_id,started_at,ended_at,dungeon,stage,difficulty,party_type,duration_minutes,note,spot\n",
            encoding="utf-8"
        )
    if not RESULTS_CSV.exists():
        RESULTS_CSV.write_text(
            "run_id,server,character,level_start,exp_start_percent,level_end,exp_end_percent,gain_exp_percent\n",
            encoding="utf-8"
        )
    if not ERRORS_CSV.exists():
        ERRORS_CSV.write_text(
            "run_id,character,timestamp,error_message\n",
            encoding="utf-8"
        )

def read_characters() -> List[str]:
    if not CHARACTERS_FILE.exists():
        raise FileNotFoundError(f"Datei fehlt {CHARACTERS_FILE}")
    names = []
    for line in CHARACTERS_FILE.read_text(encoding="utf-8").splitlines():
        name = line.strip()
        if name:
            names.append(name)
    if not names:
        raise ValueError("characters.txt enthält keine Namen")
    return names

def validate_dungeon(dungeon: str, stage: int, difficulty: str):
    if not DUNGEONS_FILE.exists():
        raise FileNotFoundError("config/dungeons.json fehlt")
    payload = json.loads(DUNGEONS_FILE.read_text(encoding="utf-8"))
    ddefs = payload.get("dungeons", {})
    key_map = {k.lower(): k for k in ddefs.keys()}
    if dungeon.lower().strip() not in key_map:
        raise ValueError(f"Dungeon unbekannt {dungeon}")
    canonical = key_map[dungeon.lower().strip()]
    max_stage = int(ddefs[canonical].get("stages", 0))
    if stage < 1 or stage > max_stage:
        raise ValueError(f"Stage außerhalb 1..{max_stage}")
    allowed = {str(x).lower() for x in ddefs[canonical].get("difficulties", [])}
    if difficulty.lower().strip() not in allowed:
        raise ValueError(f"Schwierigkeit nicht erlaubt {difficulty}")

def fetch_html_with_retries(url: str, retries: int = 3, backoff_seconds: int = 3) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)
    raise last_exc  # type: ignore[misc]

def parse_netherworld(html: str) -> Dict[str, CharSnapshot]:
    soup = BeautifulSoup(html, "lxml")

    headers = soup.find_all("h4", class_="card-title")
    target_table = None
    for h in headers:
        text = (h.get_text(strip=True) or "")
        if SERVER_HEADING_TEXT.lower() in text.lower():
            target_table = h.find_next("table")
            break
    if target_table is None:
        raise RuntimeError("Netherworld Tabelle nicht gefunden")

    result: Dict[str, CharSnapshot] = {}
    rows = target_table.find_all("tr")
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        # Robust auswählen
        # name ist die erste Zelle, die reinen Text ohne Prozentzeichen enthält
        name = tds[1].get_text(strip=True)

        level_text = tds[2].get_text(strip=True)
        exp_text = tds[4].get_text(strip=True)

        if not name:
            continue
        try:
            level = int(level_text)
        except Exception:
            continue

        m = re.match(r"^\s*([0-9]*\.?[0-9]+)\s*%\s*$", exp_text)
        if not m:
            continue
        exp_pct = float(m.group(1))  # ".08" wird 0.08

        result[name] = CharSnapshot(name, level, exp_pct)

    return result

def write_error(run_id: str, character: str, message: str):
    with ERRORS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([run_id, format_ts(now_berlin()), character + " Fehler " + message])

def append_results(
    run_id: str,
    server: str,
    rows: List[Tuple[str, Optional[int], Optional[float], Optional[int], Optional[float], Optional[float]]]
):
    # Werte mit bis zu vier Nachkommastellen speichern
    def fmt(x: Optional[float]) -> str:
        return "" if x is None else f"{x:.4f}".rstrip("0").rstrip(".")
    with RESULTS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for c, l0, e0, l1, e1, g in rows:
            w.writerow([
                run_id,
                server,
                c,
                "" if l0 is None else l0,
                fmt(e0),
                "" if l1 is None else l1,
                fmt(e1),
                fmt(g)
            ])

def append_run(
    run_id: str,
    started_at: str,
    ended_at: str,
    dungeon: str,
    stage: int,
    difficulty: str,
    party_type: str,
    duration: int,
    note: str,
    spot: str
):
    with RUNS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([run_id, started_at, ended_at, dungeon, stage, difficulty, party_type, duration, note, spot])

def main():
    parser = argparse.ArgumentParser(description="Netherworld EXP Tracker")
    parser.add_argument("--dungeon", required=True)
    parser.add_argument("--stage", required=True, type=int)
    parser.add_argument("--difficulty", required=True)
    parser.add_argument("--spot", required=True)
    parser.add_argument("--party-type", required=True)
    parser.add_argument("--note", default="")
    parser.add_argument("--sleep-seconds", type=int, default=3600)
    args = parser.parse_args()

    ensure_files()
    validate_dungeon(args.dungeon, args.stage, args.difficulty)
    characters = read_characters()

    started_dt = now_berlin()
    run_id = run_id_from_start(started_dt)
    started_text = format_ts(started_dt)

    # Lock mit Ablaufzeit
    lock_file = DATA_DIR / ".lock_active"
    if lock_file.exists():
        try:
            content = lock_file.read_text(encoding="utf-8").strip()
            parts = content.split("|")
            ts_old = datetime.fromisoformat(parts[1]).astimezone(BERLIN) if len(parts) > 1 else started_dt
        except Exception:
            ts_old = started_dt - timedelta(hours=2)
        if started_dt - ts_old < timedelta(minutes=90):
            print("Es läuft bereits ein Tracking", file=sys.stderr)
            sys.exit(1)
    lock_file.write_text(f"{run_id}|{started_dt.isoformat()}", encoding="utf-8")

    try:
        html = fetch_html_with_retries(RANKING_URL)
        snap0 = parse_netherworld(html)

        # Startwerte sammeln
        start_map: Dict[str, CharSnapshot] = {}
        for c in characters:
            if c in snap0:
                start_map[c] = snap0[c]
            else:
                write_error(run_id, c, "Name beim ersten Abruf nicht gefunden")

        # Warten
        sleep_seconds = max(1, int(args.sleep_seconds))
        time.sleep(sleep_seconds)

        # Zweiter Abruf
        html = fetch_html_with_retries(RANKING_URL)
        snap1 = parse_netherworld(html)

        # Für jeden Char eine Zeile schreiben
        rows: List[Tuple[str, Optional[int], Optional[float], Optional[int], Optional[float], Optional[float]]] = []
        for c in characters:
            s0 = start_map.get(c)
            s1 = snap1.get(c)
            l0 = s0.level if s0 else None
            e0 = s0.exp_pct if s0 else None
            l1 = s1.level if s1 else None
            e1 = s1.exp_pct if s1 else None
            g = None
            if e0 is not None and e1 is not None:
                g = e1 - e0
            if s0 is None:
                write_error(run_id, c, "Kein Startwert")
            if s1 is None:
                write_error(run_id, c, "Kein Endwert")
            rows.append((c, l0, e0, l1, e1, g))

        ended_dt = now_berlin()
        ended_text = format_ts(ended_dt)
        duration = int((ended_dt - started_dt).total_seconds() // 60)

        append_results(run_id, "Netherworld", rows)
        append_run(run_id, started_text, ended_text, args.dungeon, args.stage, args.difficulty, args.party_type, duration, args.note, args.spot)

        print(f"Lauf abgeschlossen {run_id} Zeilen {len(rows)}")
    except Exception as e:
        ended_dt = now_berlin()
        ended_text = format_ts(ended_dt)
        duration = int((ended_dt - started_dt).total_seconds() // 60)
        try:
            append_run(run_id, started_text, ended_text, args.dungeon, args.stage, args.difficulty, args.party_type, duration, f"Fehler {e}", args.spot)
        finally:
            print(f"Fehler aufgetreten {e}", file=sys.stderr)
            sys.exit(2)
    finally:
        try:
            lock_file.unlink(missing_ok=True)
        except Exception:
            pass

if __name__ == "__main__":
    main()
