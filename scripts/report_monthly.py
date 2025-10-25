#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"

RUNS = DATA_DIR / "runs.csv"
RESULTS = DATA_DIR / "results_per_char.csv"

def load_csvs():
    runs = pd.read_csv(RUNS, dtype=str)
    res = pd.read_csv(RESULTS, dtype=str)
    # cast
    for col in ["stage", "duration_minutes"]:
        if col in runs.columns:
            runs[col] = pd.to_numeric(runs[col], errors="coerce")
    for col in ["level_start", "level_end"]:
        if col in res.columns:
            res[col] = pd.to_numeric(res[col], errors="coerce")
    for col in ["exp_start_percent", "exp_end_percent", "gain_exp_percent"]:
        if col in res.columns:
            res[col] = pd.to_numeric(res[col], errors="coerce")
    return runs, res

def prev_month_year(now_utc: datetime):
    y = now_utc.year
    m = now_utc.month
    if m == 1:
        return y-1, 12
    return y, m-1

def month_filter(runs: pd.DataFrame, year: int, month: int):
    # started_at Format "YYYY-MM-DD HH Uhr MM"
    dt = pd.to_datetime(runs["started_at"].str.replace(" Uhr ", ":", regex=False), errors="coerce")
    mask = (dt.dt.year == year) & (dt.dt.month == month)
    return runs[mask].copy()

def summarize(year: int, month: int, runs: pd.DataFrame, res: pd.DataFrame):
    # join
    df = res.merge(runs, on="run_id", how="inner", suffixes=("_char", "_run"))
    # nur gültige gain
    df_valid = df[~df["gain_exp_percent"].isna()].copy()

    # Kennzahl: durchschnittlicher Δ% pro Char und Run – für Spot und Dungeon aggregieren wir über Runs (Mittelwert)
    spot_avg = df_valid.groupby(["spot","difficulty","party_type"], dropna=False)["gain_exp_percent"].mean().reset_index().sort_values("gain_exp_percent", ascending=False)
    dungeon_avg = df_valid.groupby(["dungeon","difficulty"], dropna=False)["gain_exp_percent"].mean().reset_index().sort_values("gain_exp_percent", ascending=False)

    # Char Ranking
    char_avg = df_valid.groupby(["character"])["gain_exp_percent"].mean().reset_index().sort_values("gain_exp_percent", ascending=False)

    # Top-Runs (beste Einzel-Session je spot)
    top_runs = df_valid.sort_values("gain_exp_percent", ascending=False).head(10)[
        ["run_id","character","dungeon","stage","difficulty","spot","party_type","gain_exp_percent","started_at","ended_at"]
    ]

    # Output Markdown
    month_str = f"{year}-{month:02d}"
    md = []
    md.append(f"# Netherworld EXP – Monatsreport {month_str}")
    md.append("")
    md.append("**Highlights**")
    md.append("")
    if not char_avg.empty:
        md.append(f"- Bester Char Ø Δ%: **{char_avg.iloc[0]['character']}** mit **{char_avg.iloc[0]['gain_exp_percent']:.4f}%**")
    if not spot_avg.empty:
        md.append(f"- Bester Spot Ø Δ%: **{spot_avg.iloc[0]['spot']}** ({spot_avg.iloc[0]['difficulty']}) mit **{spot_avg.iloc[0]['gain_exp_percent']:.4f}%**")
    if not dungeon_avg.empty:
        md.append(f"- Bestes Dungeon Ø Δ%: **{dungeon_avg.iloc[0]['dungeon']}** ({dungeon_avg.iloc[0]['difficulty']}) mit **{dungeon_avg.iloc[0]['gain_exp_percent']:.4f}%**")
    md.append("")

    def table(df, cols, header):
        md.append(f"## {header}")
        md.append("")
        if df.empty:
            md.append("_Keine Daten._")
            md.append("")
            return
        # einfache Markdown-Tabelle
        head = "| " + " | ".join(cols) + " |"
        sep = "
