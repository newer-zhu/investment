"""Convert daily bars CSV to qlib-compatible format.

This script helps you import your own daily bar data (OHLCV) into qlib.
It generates the calendar and daily bar files needed by qlib.

Usage:
    python scripts/prepare_qlib_data.py \\
        --input-dir /path/to/your/daily/csv \\
        --output-dir ~/.qlib/qlib_data/cn_data

Each CSV file should have columns: date,code,open,high,low,close,volume
"""

import sys
from pathlib import Path

proj_root = Path(__file__).resolve().parents[1]
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))

import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import pickle


def prepare_qlib_data(input_dir, output_dir):
    """Prepare qlib-compatible calendar and daily bar data.
    
    Args:
        input_dir: directory with daily bar CSVs (one per date or all in one file)
        output_dir: qlib data directory (default: ~/.qlib/qlib_data/cn_data)
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Input directory: {input_path}")
    print(f"Output directory: {output_path}")
    
    # Find CSV files
    csv_files = sorted(input_path.glob("*.csv"))
    if not csv_files:
        print(f"ERROR: No CSV files found in {input_path}")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Read and concatenate all data
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            dfs.append(df)
            print(f"  Read {csv_file.name}: {len(df)} rows")
        except Exception as e:
            print(f"  ERROR reading {csv_file.name}: {e}")
    
    if not dfs:
        print("ERROR: No valid CSV data loaded")
        sys.exit(1)
    
    data = pd.concat(dfs, ignore_index=True)
    
    # Normalize column names
    data.columns = [c.lower().strip() for c in data.columns]
    
    # Ensure date format
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'])
    else:
        print("ERROR: No 'date' column found in CSV")
        sys.exit(1)
    
    # Normalize code to 6-digit string with prefix
    if 'code' not in data.columns:
        print("ERROR: No 'code' column found in CSV")
        sys.exit(1)
    
    def normalize_code(c):
        s = str(c).strip()
        s = ''.join(c for c in s if c.isdigit())
        s = s.zfill(6)
        prefix = 'SH' if s.startswith('6') else 'SZ'
        return prefix + s
    
    data['code'] = data['code'].apply(normalize_code)
    
    # Extract unique dates for calendar
    dates = sorted(data['date'].unique())
    calendar = [d.strftime('%Y-%m-%d') for d in dates]
    
    print(f"\nPrepared data:")
    print(f"  Date range: {calendar[0]} to {calendar[-1]}")
    print(f"  Unique stocks: {data['code'].nunique()}")
    print(f"  Total days: {len(calendar)}")
    
    # Create calendar file (qlib requires this)
    calendar_dir = output_path / 'calendars'
    calendar_dir.mkdir(parents=True, exist_ok=True)
    calendar_file = calendar_dir / 'day.txt'
    with open(calendar_file, 'w') as f:
        f.write('\n'.join(calendar))
    print(f"\nWrote calendar: {calendar_file}")
    
    # Create instruments file
    instruments_file = output_path / 'instruments' / 'all.txt'
    instruments_file.parent.mkdir(parents=True, exist_ok=True)
    instruments = sorted(data['code'].unique())
    with open(instruments_file, 'w') as f:
        f.write('\n'.join(instruments))
    print(f"Wrote instruments: {instruments_file}")
    
    # Create daily bar data per instrument (parquet format or pickle)
    # For simplicity, we'll save as CSV in the standard qlib directory structure
    features_dir = output_path / 'features' / 'day'
    features_dir.mkdir(parents=True, exist_ok=True)
    
    for code in instruments:
        code_data = data[data['code'] == code].copy()
        code_data = code_data.sort_values('date')
        
        # Map OHLCV to qlib format
        cols = ['open', 'high', 'low', 'close', 'volume']
        out_df = code_data[['date'] + cols].set_index('date')
        
        # Save as pickle (qlib standard format)
        out_file = features_dir / f'{code}.pkl'
        with open(out_file, 'wb') as f:
            pickle.dump(out_df, f)
    
    print(f"Wrote {len(instruments)} daily bar files to {features_dir}")
    print("\n" + "=" * 60)
    print("Data preparation complete!")
    print("=" * 60)
    print(f"\nYou can now run:")
    print(f"  python -u examples/lgb_topk.py \\")
    print(f"    --pool data/stock_pool/processed/2026-01-08_pool.csv \\")
    print(f"    --topk 5 --out_dir data/pool_results_lgb")


def main():
    parser = argparse.ArgumentParser(description="Prepare qlib-compatible data from daily bar CSVs")
    parser.add_argument("--input-dir", required=True, help="input CSV directory")
    parser.add_argument("--output-dir", default=None, help="output qlib data dir (default: ~/.qlib/qlib_data/cn_data)")
    args = parser.parse_args()
    
    output_dir = args.output_dir or str(Path.home() / '.qlib' / 'qlib_data' / 'cn_data')
    prepare_qlib_data(args.input_dir, output_dir)


if __name__ == "__main__":
    main()
