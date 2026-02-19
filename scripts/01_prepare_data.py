"""
Prepare local_combined_states.csv for S3 ingestion.
- Adds canonical_id column (mth_{state_abbrev}_{sequence})
- Normalizes column names to snake_case (Athena-friendly)
- Casts nlcd_code to nullable Int64 (prevents 21.0 float in CSV)
- Outputs both CSV and TSV versions
"""

import os
from pathlib import Path

import pandas as pd

# State abbreviation mapping
STATE_ABBREV = {
    "Georgia": "ga",
    "Alabama": "al",
    "Florida": "fl",
}


def generate_canonical_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Generate mth_{state}_{sequence} IDs, ordered by state then original row."""
    df = df.copy()
    df["canonical_id"] = ""
    for state_name, abbrev in STATE_ABBREV.items():
        mask = df["state_name"] == state_name
        count = mask.sum()
        ids = [f"mth_{abbrev}_{str(i).zfill(4)}" for i in range(1, count + 1)]
        df.loc[mask, "canonical_id"] = ids
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to lowercase snake_case for Athena compatibility."""
    col_map = {
        "ID": "source_id",
        "REgion": "region",
        "NeedsUpdating": "needs_updating",
        "City": "city",
        "state_name": "state_name",
        "CityState": "city_state",
        "county_name": "county_name",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "population": "population",
        "NLCD Code": "nlcd_code",
        "ActualDescription": "land_use_description",
        "Terrain": "terrain",
        "Climate": "climate",
        "CostOfLiving": "cost_of_living",
        "MInMilesMountains": "miles_to_mountains",
        "min Drive Time Mountains": "drive_time_mountains",
        "MilesTo_Atlantic": "miles_to_atlantic",
        "MilesTo_Gulf": "miles_to_gulf",
        "Min Miles to Beach": "miles_to_beach",
        "Min Drive to Beach": "drive_time_beach",
        "MinLake": "miles_to_lake",
        "Drivelake": "drive_time_lake",
        "ClosestInternationalIATA": "closest_intl_airport_iata",
        "ClosestInternationalMiles": "closest_intl_airport_miles",
        "ClosestInternationalHours": "closest_intl_airport_hours",
        "ClosestNationalIATA": "closest_natl_airport_iata",
        "ClosestNationalMiles": "closest_natl_airport_miles",
        "ClosestNationalHours": "closest_natl_airport_hours",
        "ClosestRegionalIATA": "closest_regional_airport_iata",
        "ClosestRegionalHours": "closest_regional_airport_hours",
        "ClosestRegionalMiles": "closest_regional_airport_miles",
    }
    df = df.rename(columns=col_map)
    return df


def main():
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / "local_combined_states.csv"
    output_dir = project_root / "data" / "prepared"
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    df = normalize_columns(df)
    df = generate_canonical_ids(df)

    # Cast nlcd_code to nullable integer (prevents 21.0 float in CSV output)
    df["nlcd_code"] = df["nlcd_code"].astype("Int64")

    # Move canonical_id to first column
    cols = ["canonical_id"] + [c for c in df.columns if c != "canonical_id"]
    df = df[cols]

    # Save CSV (for Athena registration)
    csv_path = output_dir / "mth_communities.csv"
    df.to_csv(csv_path, index=False)
    print(f"Wrote CSV: {csv_path} ({len(df)} rows)")

    # Save TSV (alternative format)
    tsv_path = output_dir / "mth_communities.tsv"
    df.to_csv(tsv_path, index=False, sep="\t")
    print(f"Wrote TSV: {tsv_path} ({len(df)} rows)")

    # Print sample canonical IDs
    for state in STATE_ABBREV:
        sample = df[df["state_name"] == state]["canonical_id"].head(3).tolist()
        print(f"  {state}: {sample}")

    print(f"\nColumn list ({len(df.columns)}):")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")

    # Verify no duplicates
    dupes = df["canonical_id"].duplicated().sum()
    print(f"\nDuplicate canonical_ids: {dupes}")
    assert dupes == 0, "Found duplicate canonical IDs!"


if __name__ == "__main__":
    main()
