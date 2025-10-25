#!/usr/bin/env python3
import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import re

import requests
from bs4 import BeautifulSoup

RANKING_URL = "https://pr-underworld.com/website/ranking/"
SERVER_HEADING_TEXT = "Netherworld"

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"

CHARACTERS_FILE = CONFIG_DIR / "characters.txt"
DUNGEONS_FILE = CONFIG_DIR / "dungeons.json"

RUNS_CSV = DATA_DIR / "runs.csv"
RESULTS_CSV = DATA_DIR / "results_per_char.csv"
ERRORS_CSV = DATA_DIR / "errors.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (EXP-Tracker; +github-actions)"
}

@dataclass
class CharSnapshot:
    character: str
    level: int
    exp_pct: float

def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H Uhr %M")

def run_id_from_start(ts: datetime) -> str:
    return ts.strftime("R%Y%m%d_%H%M%S")

def ensure_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not RUNS_CSV.exists():
        RUNS_CSV.write_text("run_id,started_at,ended_at,dungeon,stage,difficulty,party_type,duration_minutes,note\n", encoding="utf-8")
    if not RESULTS_CSV.exists():
        RESULTS_CSV.write_text("run_id,server,character,level_start,exp_start_percent,level_end,exp_end_percent,gain_exp_percent\n", encoding="utf-8")
    if not ERRORS_CSV.exists():
        ERRORS_CSV.write_text("run_id,character,timestamp,error_message\n", encoding="utf-8")

def read_characters() -> list[str]:
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
        return
    payload = json.loads(DUNGEONS_FILE.read_text(encoding="utf-8"))
    ddefs = payload.get("dungeons", {})
    if dungeon not in ddefs:
        raise ValueError(f"Dungeon unbekannt {dungeon}")
    max_stage = int(ddefs[dungeon].get("stages", 0))
    if stage < 1 or stage > max_stage:
        raise ValueError(f"Stage außerhalb 1..{max_stage}")
    allowed = {str(x) for x in ddefs[dungeon].get("difficulties", [])}
    if str(difficulty) not in allowed:
        raise ValueError(f"Schwierigkeit nicht erlaubt {difficulty}")

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r.text

def parse_netherworld(html: str) -> dict[str, CharSnapshot]:
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

    result: dict[str, CharSnapshot] = {}

    rows = target_table.find_all("tr")
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue
        # Spaltenindex passend zu deinem HTML Beispiel
        name = tds[1].get_text(strip=True)
        level_text = tds[2].get_text(strip=True)
        exp_text = tds[4].get_text(strip=True)

        if not name:
            continue
        try:
            level = int(level_text)
        except:
            continue

        m = re.match(r"^\s*([0-9]*\.?[0-9]+)\s*%\s*$", exp_text)
        if not m:
            continue
        exp_pct = float(m.group(1))

        result[name] = CharSnapshot(name, level, exp_pct)

    return result

def write_error(run_id: str, character: str, message: str):
    with ERRORS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([run_id, character, now_text(), message])

def append_results(run_id: str, server: str, deltas: list[tuple[str, int, float, int, float, float]]):
    with RESULTS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for c, l0, e0, l1, e1, g in deltas:
            w.writerow([run_id, server, c, l0, f"{e0:.2f}", l1, f"{e1:.2f}", f"{g:.2f}"])

def append_run(run_id: str, started_at: str, ended_at: str, dungeon: str, stage: int, difficulty: str, party_type: str, duration: int, note: str):
    with RUNS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([run_id, started_at, ended_at, dungeon, stage, difficulty, party_type, duration, note])

def main():
    parser = argparse.ArgumentParser(description="Netherworld EXP Tracker")
    parser.add_argument("--dungeon", required=True)
    parser.add_argument("--stage", required=True, type=int)
    parser.add_argument("--difficulty", required=True)
    parser.add_argument("--party-type", required=True)
    parser.add_argument("--note", default="")
    parser.add_argument("--sleep-seconds", type=int, default=3600)
    args = parser.parse_args()

    ensure_files()
    validate_dungeon(args.dungeon, args.stage, args.difficulty)
    characters = read_characters()

    started_dt = datetime.now()
    run_id = run_id_from_start(started_dt)
    started_text = now_text()

    lock_file = DATA_DIR / ".lock_active"
    if lock_file.exists():
        print("Es läuft bereits ein Tracking", file=sys.stderr)
        sys.exit(1)
    lock_file.write_text(run_id, encoding="utf-8")

    try:
        html = fetch_html(RANKING_URL)
        snap0 = parse_netherworld(html)

        start_map = {}
        for c in characters:
            if c in snap0:
                start_map[c] = snap0[c]
            else:
                write_error(run_id, c, "Name beim ersten Abruf nicht gefunden")

        time.sleep(max(1, args.sleep_seconds))

        html = fetch_html(RANKING_URL)
        snap1 = parse_netherworld(html)

        deltas = []
        for c in characters:
            s0 = start_map.get(c)
            s1 = snap1.get(c)
            if not s0 or not s1:
                if not s1:
                    write_error(run_id, c, "Name beim zweiten Abruf nicht gefunden")
                continue
            gain = s1.exp_pct - s0.exp_pct
            deltas.append((c, s0.level, s0.exp_pct, s1.level, s1.exp_pct, gain))

        ended_text = now_text()
        duration = 60

        append_results(run_id, "Netherworld", deltas)
        append_run(run_id, started_text, ended_text, args.dungeon, args.stage, args.difficulty, args.party_type, duration, args.note)

        print(f"Lauf abgeschlossen {run_id} Einträge gespeichert {len(deltas)}")
    except Exception as e:
        ended_text = now_text()
        append_run(run_id, started_text, ended_text, args.dungeon, args.stage, args.difficulty, args.party_type, 0, f"Fehler {e}")
        print(f"Fehler aufgetreten {e}", file=sys.stderr)
        sys.exit(2)
    finally:
        try:
            lock_file.unlink(missing_ok=True)
        except Exception:
            pass

if __name__ == "__main__":
    main()
