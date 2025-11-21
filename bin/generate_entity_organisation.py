#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd


def build_entity_organisation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert lookup.csv → entity-organisation.csv with columns:
    dataset, entity-minimum, entity-maximum, organisation
    """

    # Expected columns: prefix, entity, organisation
    df = df.rename(columns={"prefix": "dataset"})

    # Drop rows with missing / placeholder organisations
    df["organisation"] = df["organisation"].astype(str)
    org_clean = df["organisation"].str.strip()
    mask_valid_org = ~org_clean.str.lower().isin(["", "nan", "none"])
    df = df[mask_valid_org].copy()

    # Require entities and cast to int
    df = df[df["entity"].notna()].copy()
    df["entity"] = df["entity"].astype(int)

    # Sort by dataset then entity (not org)
    df = df.sort_values(["dataset", "entity"])

    rows = []
    start = prev = None
    current_org = current_ds = None

    # Collapse consecutive entities into ranges per dataset + organisation
    for ds, org, ent in df[["dataset", "organisation", "entity"]].itertuples(index=False):
        if (
            org != current_org
            or ds != current_ds
            or prev is None
            or ent != prev + 1
        ):
            if start is not None:
                rows.append([current_ds, start, prev, current_org])
            start = ent
            current_org = org
            current_ds = ds
        prev = ent

    if start is not None:
        rows.append([current_ds, start, prev, current_org])

    out = pd.DataFrame(
        rows,
        columns=["dataset", "entity-minimum", "entity-maximum", "organisation"],
    )
    return out.sort_values(["dataset", "entity-minimum"]).reset_index(drop=True)


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

    # Only match root/*/lookup.csv (one folder deep)
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
            df = pd.read_csv(lookup, dtype=str)
            out = build_entity_organisation(df)
            output_path = lookup.with_name("entity-organisation.csv")
            out.to_csv(output_path, index=False)
            print(f"  ✓ Wrote {len(out)} rows → {output_path.name}")
        except Exception as e:
            print(f"  ✗ Failed for {pipeline}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()