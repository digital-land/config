#!/usr/bin/env python3
import csv
import re
from pathlib import Path

TARGET_DIRS = ("pipeline", "specification", "collection")

# Matches malformed form: 2023-06-23T10:10:11:49Z
BAD_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2}):(\d{1,6})Z$"
)

def normalize_timestamp(value: str) -> str:
    if not isinstance(value, str):
        return value
    s = value.strip()
    m = BAD_TS_RE.match(s)
    if not m:
        return value

    date_part, hh, mm, ss, frac = m.groups()
    # Convert trailing part to milliseconds (pad/truncate to 3 digits)
    ms = frac.ljust(3, "0")[:3]
    return f"{date_part}T{hh}:{mm}:{ss}.{ms}Z"

def fix_csv_file(csv_path: Path) -> int:
    changed = 0
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if not rows:
        return 0

    for row in rows:
        for k, v in row.items():
            new_v = normalize_timestamp(v)
            if new_v != v:
                row[k] = new_v
                changed += 1

    if changed:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return changed

def main(root="."):
    root_path = Path(root)
    total_files_changed = 0
    total_values_changed = 0

    for target_dir in TARGET_DIRS:
        base_dir = root_path / target_dir
        if not base_dir.exists() or not base_dir.is_dir():
            continue

        for csv_file in base_dir.rglob("*.csv"):
            changes = fix_csv_file(csv_file)
            if changes:
                total_files_changed += 1
                total_values_changed += changes
                print(f"Updated {csv_file} ({changes} value(s))")

    print(
        f"Done. Files changed: {total_files_changed}, "
        f"timestamps fixed: {total_values_changed}"
    )

if __name__ == "__main__":
    main(".")