"""Helpers to convert daily stock-pool CSVs into qlib-friendly pool files.

This module provides `convert_daily_pool` which:
- reads a daily CSV (no date column; date inferred from filename if possible),
- normalizes stock codes to 6-digit strings,
- maps to exchange prefixes (simple rule: codes starting with '6' -> 'SH', else 'SZ'),
- writes an output CSV with columns `date,code,instrument` and a simple one-column
  file `{date}_pool.csv` containing `instrument` (one per line) which can be
  used as a stock pool listing for downstream workflows.

Usage:
    from src.qlib_project.pool_utils import convert_daily_pool
    convert_daily_pool("data/stock_pool/picked_stocks_20260108.csv", "data/stock_pool/processed")

"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import pandas as pd


def _infer_date_from_filename(path: str) -> Optional[str]:
    # look for YYYYMMDD or YYYY-MM-DD
    fn = os.path.basename(path)
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", fn)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", fn)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _normalize_code(code: str) -> str:
    s = str(code).strip()
    # Remove non-digits
    s = re.sub(r"\D", "", s)
    return s.zfill(6)


def _map_exchange_prefix(code6: str) -> str:
    # Simple rule for China A-shares: starting with '6' -> SH, else -> SZ
    return "SH" if code6.startswith("6") else "SZ"


def convert_daily_pool(
    csv_path: str,
    out_dir: str,
    date: Optional[str] = None,
    code_column: str = "代码",
    write_onecol: bool = True,
    instrument_sep: str = "",
):
    """Convert a daily stock-pool CSV to a qlib-friendly format.

    Args:
        csv_path: path to the input CSV (e.g., picked_stocks_20260108.csv)
        out_dir: directory where processed files will be written
        date: optional date string (YYYY-MM-DD). If None, inferred from filename.
        code_column: name of the column with stock codes in the CSV (default '代码')
        write_onecol: whether to write a `{date}_pool.csv` with a single `instrument` column
        instrument_sep: separator between prefix and code, set to '.' if needed (e.g., 'SH.600000')

    Returns:
        path to the main processed CSV (with columns `date,code,instrument`).
    """
    p = Path(csv_path)
    if date is None:
        date = _infer_date_from_filename(p.name)
    if date is None:
        raise ValueError("Date not provided and could not be inferred from filename.")

    df = pd.read_csv(p, dtype=str)
    if code_column not in df.columns:
        # try common alternatives
        for alt in ["code", "代码", "代码\n", "证券代码", "ticker"]:
            if alt in df.columns:
                code_column = alt
                break
    if code_column not in df.columns:
        raise ValueError(f"Code column '{code_column}' not found in CSV. Available: {list(df.columns)}")

    codes = df[code_column].astype(str).map(_normalize_code)
    prefixes = codes.map(_map_exchange_prefix)
    instruments = prefixes + instrument_sep + codes

    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    out_df = pd.DataFrame({"date": date, "code": codes, "instrument": instruments})

    main_out = out_dir_p / f"{date}_pool_full.csv"
    out_df.to_csv(main_out, index=False, encoding="utf-8")

    if write_onecol:
        onecol = out_dir_p / f"{date}_pool.csv"
        out_df["instrument"].to_csv(onecol, index=False, header=True, encoding="utf-8")

    return str(main_out)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert daily stock pool CSV to qlib-friendly pool files")
    parser.add_argument("csv_path")
    parser.add_argument("out_dir")
    parser.add_argument("--date", default=None)
    parser.add_argument("--code-column", default="代码")
    parser.add_argument("--no-onecol", dest="onecol", action="store_false")
    parser.add_argument("--sep", default="")
    args = parser.parse_args()
    print(convert_daily_pool(args.csv_path, args.out_dir, date=args.date, code_column=args.code_column, write_onecol=args.onecol, instrument_sep=args.sep))
