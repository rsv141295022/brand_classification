"""Combine raw data from datasets/1_raw_data into datasets/2_master_data."""

import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "datasets" / "1_raw_data"
MASTER_DIR = BASE / "datasets" / "2_master_data"
OUTPUT_FILE = MASTER_DIR / "master_data.csv"

# Read all CSV and XLSX files from raw
dfs = []
for f in sorted(RAW_DIR.glob("*.csv")):
    dfs.append(pd.read_csv(f))
    print(f"  Read {f.name}: {len(dfs[-1])} rows")
for f in sorted(RAW_DIR.glob("*.xlsx")):
    dfs.append(pd.read_excel(f))
    print(f"  Read {f.name}: {len(dfs[-1])} rows")

if not dfs:
    print("No CSV or XLSX files found in 1_raw_data")
    exit(1)

# Combine and deduplicate
combined = pd.concat(dfs, ignore_index=True)
before = len(combined)
combined = combined.drop_duplicates(subset=["group_name", "parent_company"])
combined = combined.sort_values(["parent_company", "group_name"]).reset_index(drop=True)

# Write to master
MASTER_DIR.mkdir(parents=True, exist_ok=True)
combined.to_csv(OUTPUT_FILE, index=False)

print(f"\nCombined: {before} -> {len(combined)} rows (after dedup)")
print(f"Output: {OUTPUT_FILE}")
