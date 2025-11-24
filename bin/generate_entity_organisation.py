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

    # Ensure organisation is not blank
    df["organisation"] = df["organisation"].astype(str)
    df = df[df["organisation"].str.strip() != ""].copy()

    # Require valid entity numbers
    df = df[df["entity"].notna()].copy()
    df["entity"] = df["entity"].astype(int)

    # Sort for deterministic grouping
    df = df.sort_values(["dataset", "organisation", "entity"])

    # -------------------------------------------------------
    # Improved automatic contiguous range grouping
    # -------------------------------------------------------
    df["range_group"] = (
        df.groupby(["dataset", "organisation"])["entity"]
          .apply(lambda s: (s.diff() != 1).cumsum())
          .astype(int)
    )

    # Compute min/max per range
    out = (
        df.groupby(["dataset", "organisation", "range_group"])
          .agg(entity_minimum=("entity", "min"),
               entity_maximum=("entity", "max"))
          .reset_index(drop=True)
    )

    return out.sort_values(["dataset", "organisation", "entity_minimum"]).reset_index(drop=True)


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

    # Only match root/*/lookup.csv
    lookup_files = sorted(root.glob("*/lookup.csv"))

    # If --only is supplied, filter to those pipeline names
    if args.only:
        wanted = set(args.only)
        lookup_files = [p for p in lookup_files if p.parent.name in wanted]
    
    if not lookup_files:
        print("No lookup.csv files found.")
        return

    for lookup in lookup_files:
        pipeline = lookup.parent.name
        print(f"Processing {pipeline}...")

        try:
            df = pd.read_csv(lookup)
            out = build_entity_organisation(df)
            output_path = lookup.with_name("entity-organisation.csv")
            out.to_csv(output_path, index=False)
            print(f"  ✓ Wrote {len(out)} rows → {output_path.name}")
        except Exception as e:
            print(f"  ✗ Failed for {pipeline}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
