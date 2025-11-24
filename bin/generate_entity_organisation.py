#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd


def build_entity_organisation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert lookup.csv → entity-organisation.csv with columns:
    dataset, entity-minimum, entity-maximum, organisation
    """

    # Rename prefix → dataset (matches your existing logic)
    df = df.rename(columns={"prefix": "dataset"})

    # Ensure organisation is not blank or placeholder
    df["organisation"] = df["organisation"].astype(str)
    org_clean = df["organisation"].str.strip()
    df = df[~org_clean.str.lower().isin(["", "nan", "none"])].copy()

    # Require valid entity numbers
    df = df[df["entity"].notna()].copy()
    df["entity"] = df["entity"].astype(int)

    # Sort for deterministic grouping
    df = df.sort_values(["dataset", "organisation", "entity"])

    # -------------------------------------------------------
    # Contiguous range grouping (fixed: use transform, not apply)
    # -------------------------------------------------------
    df["range_group"] = (
        df.groupby(["dataset", "organisation"])["entity"]
          .transform(lambda s: (s.diff() != 1).cumsum())
          .astype(int)
    )

    # Compute min/max per range
    grouped = (
        df.groupby(["dataset", "organisation", "range_group"])["entity"]
          .agg(entity_minimum="min", entity_maximum="max")
          .reset_index()
    )

    # Build output
    out = grouped[
        ["dataset", "entity_minimum", "entity_maximum", "organisation"]
    ].rename(
        columns={
            "entity_minimum": "entity-minimum",
            "entity_maximum": "entity-maximum",
        }
    )

    return out.sort_values(["dataset", "organisation", "entity-minimum"]).reset_index(drop=True)


def detect_overlapping_ranges(entity_ranges: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame of ranges with columns:
      dataset, entity-minimum, entity-maximum, organisation

    Return a DataFrame listing any overlapping ranges per dataset.
    """
    if entity_ranges.empty:
        return pd.DataFrame()

    df = entity_ranges.copy()
    df = df.sort_values(["dataset", "entity-minimum"])

    overlaps = []
    prev = None

    for _, row in df.iterrows():
        if prev is not None and row["dataset"] == prev["dataset"]:
            # Overlap when the next range starts before (or on) the previous max
            if int(row["entity-minimum"]) <= int(prev["entity-maximum"]):
                overlaps.append(
                    {
                        "dataset": row["dataset"],
                        "current-entity-minimum": row["entity-minimum"],
                        "current-entity-maximum": row["entity-maximum"],
                        "current-organisation": row["organisation"],
                        "previous-entity-minimum": prev["entity-minimum"],
                        "previous-entity-maximum": prev["entity-maximum"],
                        "previous-organisation": prev["organisation"],
                    }
                )
        prev = row

    return pd.DataFrame(overlaps)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Folder containing pipeline directories (eg pipeline/)",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        help="Only process specific pipeline directory names (eg --only listed-building).",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    print(f"Searching for lookup.csv under: {root}")

    lookup_files = sorted(root.glob("*/lookup.csv"))

    # Only process those specified with --only
    if args.only:
        wanted = set(args.only)
        lookup_files = [p for p in lookup_files if p.parent.name in wanted]

    if not lookup_files:
        print("No lookup.csv files found.")
        return

    all_overlaps = []

    for lookup in lookup_files:
        pipeline = lookup.parent.name
        print(f"\nProcessing {pipeline}...")

        try:
            df = pd.read_csv(lookup, dtype=str)
            out = build_entity_organisation(df)

            # ----------------------------------------
            # Detect overlaps BEFORE writing the file
            # ----------------------------------------
            overlaps = detect_overlapping_ranges(out)

            if not overlaps.empty:
                overlaps.insert(0, "pipeline", pipeline)
                all_overlaps.append(overlaps)
                print(f"  ⚠ Overlaps detected — NOT writing entity-organisation.csv for {pipeline}")
                continue  # Skip producing entity-organisation.csv

            # No overlaps → Safe to write file
            output_path = lookup.with_name("entity-organisation.csv")
            out.to_csv(output_path, index=False)
            print(f"  ✓ Wrote {len(out)} rows → {output_path.name}")
            print("  ✓ No overlaps found.")

        except Exception as e:
            print(f"  ✗ Failed for {pipeline}: {e}")

    # After all pipelines, write combined overlaps file
    if all_overlaps:
        combined = pd.concat(all_overlaps, ignore_index=True)
        script_dir = Path(__file__).resolve().parent
        combined_path = script_dir / "overlapping-entity-ranges.csv"
        combined.to_csv(combined_path, index=False)
        print(f"\n⚠ Wrote {len(combined)} overlapping ranges to {combined_path}")
    else:
        print("\n✓ No overlapping ranges found in any pipeline.")

    print("\nDone.")


if __name__ == "__main__":
    main()
