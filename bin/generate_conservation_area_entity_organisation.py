#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

PREFERRED_DATASET = "conservation-area"

GOV_PREFIX = "government-organisation:"
LA_PREFIX = "local-authority:"
GLA_ORG = "local-authority:GLA"

GOV_PB1164 = "government-organisation:PB1164"
GOV_D1342 = "government-organisation:D1342"


def load_lookup(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)

    # Rename prefix -> dataset (matches existing conventions)
    df = df.rename(columns={"prefix": "dataset"})

    # Keep conservation-area only – ignore conservation-area-document etc
    df = df[df["dataset"] == PREFERRED_DATASET].copy()

    # Clean organisation
    df["organisation"] = df["organisation"].astype(str).str.strip()
    df = df[~df["organisation"].isin(["", "nan", "NaN", "None", "none"])].copy()

    # Clean entity
    df = df[df["entity"].notna()].copy()
    df["entity"] = df["entity"].astype(int)

    return df


def pick_organisation_for_entity(entity: int, orgs: list[str]) -> str:
    """
    Decide which organisation to keep for a single entity, using your rules:

    - If only one unique organisation -> pick it.
    - Always pick a non-government organisation (anything not starting
      'government-organisation:') over a government organisation.
    - Local-authority:
        * If multiple distinct local-authority:* entries (excluding GLA)
          -> error.
        * If exactly one non-GLA LA (possibly plus GLA) -> pick that LA.
        * If only GLA as an LA, treat it like any other non-gov and fall back
          to "pick a deterministic non-gov".
    - If there are no non-government organisations:
        * Prefer government-organisation:PB1164 over government-organisation:D1342.
        * Otherwise pick the first sorted government org.
    """
    # Deduplicate and normalise
    uniq = sorted(
        {
            o.strip()
            for o in orgs
            if o
            and str(o).strip() not in ["", "nan", "NaN", "None", "none"]
        }
    )

    if not uniq:
        raise ValueError(f"No valid organisations found for entity {entity}")

    if len(uniq) == 1:
        return uniq[0]

    # Separate gov and non-gov
    non_gov = [o for o in uniq if not o.startswith(GOV_PREFIX)]
    gov = [o for o in uniq if o.startswith(GOV_PREFIX)]

    # Prefer non-government organisations over government ones
    if non_gov:
        # Focus on local authorities for the special case
        la_orgs = [o for o in non_gov if o.startswith(LA_PREFIX)]

        if la_orgs:
            # Ignore GLA when there is another LA
            non_gla_las = [o for o in la_orgs if o != GLA_ORG]

            if len(non_gla_las) > 1:
                # Interactive resolution: ask the user which LA to keep
                print("\n----------------------------------------")
                print(f"Entity {entity} has multiple local authorities:")
                for i, o in enumerate(sorted(non_gla_las), start=1):
                    print(f"  {i}. {o}")
                print("----------------------------------------")
    
                while True:
                    choice = input(f"Pick the correct organisation for entity {entity} (1-{len(non_gla_las)}): ").strip()
                    if choice.isdigit() and 1 <= int(choice) <= len(non_gla_las):
                        chosen = sorted(non_gla_las)[int(choice) - 1]
                        print(f"Chosen: {chosen}")
                        return chosen
                    else:
                        print("Invalid choice. Please enter a valid number.")

            if len(non_gla_las) == 1:
                # Exactly one "real" LA (possibly plus GLA) -> that wins
                return non_gla_las[0]

            # If we get here, only LA present is GLA, so drop into generic non-gov behaviour

        # No LAs (or only GLA), just pick a deterministic non-gov winner
        return sorted(non_gov)[0]

    # No non-gov left, so pick among government organisations
    if GOV_PB1164 in gov:
        return GOV_PB1164
    if GOV_D1342 in gov:
        return GOV_D1342

    # Otherwise, just pick the first sorted gov org
    return sorted(gov)[0]


def resolve_entity_organisations(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each entity in conservation-area lookup, choose exactly one organisation.
    """
    records = []

    for entity, group in df.groupby("entity"):
        orgs = group["organisation"].tolist()
        chosen = pick_organisation_for_entity(entity, orgs)
        records.append(
            {
                "dataset": PREFERRED_DATASET,
                "entity": int(entity),
                "organisation": chosen,
            }
        )

    out = pd.DataFrame.from_records(records)
    # Order by organisation then entity – easier to see ranges
    return out.sort_values(["organisation", "entity"]).reset_index(drop=True)


def build_ranges(entity_orgs: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse single-organisation-per-entity rows into contiguous ranges per
    (dataset, organisation):

    dataset, entity-minimum, entity-maximum, organisation
    """
    df = entity_orgs.copy()
    df = df.sort_values(["dataset", "organisation", "entity"]).reset_index(drop=True)

    # Group consecutive entity values for each dataset+organisation
    df["range_group"] = (
        df.groupby(["dataset", "organisation"])["entity"]
        .transform(lambda s: (s.diff() != 1).cumsum())
        .astype(int)
    )

    grouped = (
        df.groupby(["dataset", "organisation", "range_group"])["entity"]
        .agg(entity_minimum="min", entity_maximum="max")
        .reset_index()
    )

    out = grouped[
        ["dataset", "entity_minimum", "entity_maximum", "organisation"]
    ].rename(
        columns={
            "entity_minimum": "entity-minimum",
            "entity_maximum": "entity-maximum",
        }
    )

    # Final order: dataset, organisation, entity-minimum
    return out.sort_values(
        ["dataset", "organisation", "entity-minimum"]
    ).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default="pipeline",
        help="Root folder containing 'conservation-area/lookup.csv' (default: pipeline/)",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    lookup_path = root / "conservation-area" / "lookup.csv"

    print(f"Root directory: {root}")
    print(f"Reading lookup from: {lookup_path}")

    if not lookup_path.exists():
        raise SystemExit(f"lookup.csv not found at {lookup_path}")

    df_lookup = load_lookup(lookup_path)
    if df_lookup.empty:
        raise SystemExit("No valid conservation-area rows found in lookup.csv")

    print(f"Loaded {len(df_lookup)} lookup rows for {PREFERRED_DATASET}")

    entity_orgs = resolve_entity_organisations(df_lookup)
    print(f"Resolved {len(entity_orgs)} entities to a single organisation each")

    ranges = build_ranges(entity_orgs)
    print(f"Built {len(ranges)} contiguous entity ranges")

    output_path = lookup_path.with_name("entity-organisation.csv")
    ranges.to_csv(output_path, index=False)
    print(f"Wrote entity-organisation ranges to: {output_path}")


if __name__ == "__main__":
    main()