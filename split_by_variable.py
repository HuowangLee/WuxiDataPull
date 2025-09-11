#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script: split_by_variable.py
Usage:
    python split_by_variable.py --prefix <folder_prefix> [--outdir <output_dir>] [--pattern "*.csv"]

Description:
    - Finds all subdirectories in the current working directory that start with the given prefix.
    - Reads every CSV file in those subdirectories with GBK encoding.
    - Assumes the FIRST COLUMN is time and the first row is the header (first cell is the time header).
    - Concatenates all rows across all CSVs, sorts by time ascending.
    - For each variable (each non-time column), writes a CSV that contains only two columns:
        ["time", "<variable>"]
      into a folder named exactly by the variable (sanitized for filesystem safety).
    - Each variable folder will contain a single file named "<variable>.csv", saved in UTF-8.
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from typing import List

def find_target_dirs(prefix: str) -> List[Path]:
    cwd = Path.cwd()
    return [p for p in cwd.iterdir() if p.is_dir() and p.name.startswith(prefix)]

def iter_csv_files(d: Path, pattern: str) -> List[Path]:
    return sorted(d.rglob(pattern))

def read_csv_with_time_first(path: Path) -> pd.DataFrame:
    # 🔑 用 gbk 读取
    df = pd.read_csv(path, encoding="gbk")
    if df.shape[1] < 2:
        raise ValueError(f"{path} has fewer than 2 columns.")
    new_cols = list(df.columns)
    new_cols[0] = "time"
    df.columns = [str(c).strip() for c in new_cols]
    return df

def coerce_time(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce", utc=False, infer_datetime_format=True)
    if s.notna().mean() >= 0.8:
        return s
    s_num = pd.to_numeric(series, errors="coerce")
    if s_num.notna().mean() >= 0.8:
        return s_num
    return series.astype(str)

def sanitize_name(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", " ", ".", "#") else "_" for ch in str(name))
    return safe.strip() or "unnamed_variable"

def main():
    ap = argparse.ArgumentParser(description="Split concatenated CSVs by variable into per-variable folders.")
    ap.add_argument("--prefix", required=True, help="Prefix of subfolders to scan.")
    ap.add_argument("--outdir", default="variables_out", help="Output base directory.")
    ap.add_argument("--pattern", default="*.csv", help='Glob pattern for CSV files (default: "*.csv").')
    args = ap.parse_args()

    target_dirs = find_target_dirs(args.prefix)
    if not target_dirs:
        print(f"No directories starting with prefix: {args.prefix}", file=sys.stderr)
        sys.exit(1)

    csv_paths: List[Path] = []
    for d in target_dirs:
        csv_paths.extend(iter_csv_files(d, args.pattern))

    if not csv_paths:
        print(f"No CSV files found under directories with prefix: {args.prefix}", file=sys.stderr)
        sys.exit(1)

    frames = []
    for p in csv_paths:
        try:
            df = read_csv_with_time_first(p)
        except Exception as e:
            print(f"[WARN] Skipping {p}: {e}", file=sys.stderr)
            continue
        frames.append(df)

    if not frames:
        print("No valid CSV files were read.", file=sys.stderr)
        sys.exit(1)

    big = pd.concat(frames, axis=0, ignore_index=True, sort=False)
    big["time"] = coerce_time(big["time"])
    big = big.sort_values("time", kind="mergesort").drop_duplicates(subset=["time"], keep="last")

    out_base = Path(args.outdir)
    out_base.mkdir(parents=True, exist_ok=True)

    variable_cols = [c for c in big.columns if c != "time"]
    if not variable_cols:
        print("No variable columns found.", file=sys.stderr)
        sys.exit(1)

    for col in variable_cols:
        safe_name = sanitize_name(col)
        out_dir = out_base / safe_name
        out_dir.mkdir(parents=True, exist_ok=True)

        sub = big[["time", col]].copy()
        sub = sub.sort_values("time").groupby("time", as_index=False).agg({col: "last"})

        out_csv = out_dir / f"{safe_name}.csv"
        # 🔑 以 utf-8 保存
        sub.to_csv(out_csv, index=False, encoding="utf-8")

    print(f"Done. Wrote {len(variable_cols)} variables into '{out_base}'.")

if __name__ == "__main__":
    main()
