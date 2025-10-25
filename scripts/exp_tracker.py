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
import os

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
    exp_pct: float  # ".08" -> 0.08

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

    headers = soup.find
