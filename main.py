from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.cleaner import CleanConfig, clean_contacts_csv

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean & dedupe CRM contacts CSV")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--output", required=True, help="Output cleaned CSV path")
    p.add_argument("--report", required=True, help="Output report CSV path")
    p.add_argument("--date-field", default="last_clean_date", help="Canonical date field to keep latest")
    p.add_argument("--dedupe-key", default="email", choices=["email", "full_name"], help="Dedupe key")
    p.add_argument("--required", default="full_name", help='Comma-separated canonical required fields, e.g. "full_name,email"')
    p.add_argument("--mapping", default="", help="Optional JSON mapping file (canonical->source header).")
    return p.parse_args()

def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    rep_path = Path(args.report)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)

    mapping = None
    if args.mapping:
        mapping = json.loads(Path(args.mapping).read_text(encoding="utf-8"))

    cfg = CleanConfig(
        date_field=args.date_field,
        dedupe_key=args.dedupe_key,
        required=tuple([x.strip() for x in args.required.split(",") if x.strip()]),
        mapping=mapping,
    )

    result = clean_contacts_csv(df, cfg)
    result.cleaned.to_csv(out_path, index=False)
    result.report.to_csv(rep_path, index=False)

    print(f"✅ Cleaned rows: {len(result.cleaned)}")
    print(f"📝 Report rows: {len(result.report)}")
    print(f"Saved: {out_path}")
    print(f"Saved: {rep_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
