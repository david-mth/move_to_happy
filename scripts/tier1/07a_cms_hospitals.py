"""CMS Hospital Compare data for all MTH communities.

Downloads the CMS Hospital General Information CSV (no auth required)
and computes distance-based hospital access metrics for each of the ~1,307
communities across Georgia, Alabama, and Florida.

Target states for hospital data: GA, AL, FL plus border states
(MS, TN, NC, SC) to ensure edge communities have nearby coverage.

Output columns:
    canonical_id, nearest_hospital_name, nearest_hospital_miles,
    nearest_hospital_rating, nearest_er_miles, hospitals_within_15mi,
    hospitals_within_30mi, avg_rating_within_30mi

Usage:
    poetry run python scripts/tier1/07a_cms_hospitals.py
"""

from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    TIER1_DIR,
    api_get,
    ensure_dirs,
    haversine_miles,
    load_cached,
    load_communities,
    register_athena_table,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CMS_HOSPITAL_CSV_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "092256becd267d9dd933f8571064c1f8/Hospital_General_Information.csv"
)

# States to include: our 3 core + border states for edge community coverage
TARGET_STATES = {"GA", "AL", "FL", "MS", "TN", "NC", "SC"}

ZIPPOPOTAM_URL = "https://api.zippopotam.us/us/{zip}"

# Distance thresholds in miles
NEAR_MI = 15.0
FAR_MI = 30.0

# CMS overall rating sentinel
NOT_AVAILABLE = "Not Available"

# Athena schema
ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("nearest_hospital_name", "string"),
    ("nearest_hospital_miles", "double"),
    ("nearest_hospital_rating", "double"),
    ("nearest_er_miles", "double"),
    ("hospitals_within_15mi", "int"),
    ("hospitals_within_30mi", "int"),
    ("avg_rating_within_30mi", "double"),
]

OUTPUT_COLUMNS = [col for col, _ in ATHENA_COLUMNS]


# ---------------------------------------------------------------------------
# Download + parse CMS hospital CSV
# ---------------------------------------------------------------------------


def download_hospital_csv() -> pd.DataFrame:
    """Download CMS Hospital General Information CSV and return raw DataFrame.

    Uses a bulk cache file so subsequent runs skip the ~3 MB download.
    """
    cache_key = "hospital_data"
    cached = load_cached("cms_hospitals", cache_key)
    if cached is not None:
        print(f"  Cache hit: {len(cached)} hospital records")
        return pd.DataFrame(cached)

    print(f"  Downloading CMS hospital CSV from {CMS_HOSPITAL_CSV_URL} …")
    resp = api_get(CMS_HOSPITAL_CSV_URL, rate_limit=0.0, timeout=120)
    df = pd.read_csv(io.StringIO(resp.text), dtype=str)

    # Normalise column names: lower-case, replace spaces with underscores
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)

    save_cache("cms_hospitals", cache_key, df.to_dict(orient="records"))
    print(f"  Downloaded and cached: {len(df)} hospitals")
    return df


def parse_hospitals(raw: pd.DataFrame) -> pd.DataFrame:
    """Filter and clean the raw CMS DataFrame.

    Keeps only target-state hospitals that have usable location data.
    Returns a DataFrame with columns:
        facility_name, state, hospital_type, emergency_services,
        rating, address, city, zip_code, latitude, longitude
    """
    df = raw.copy()

    # --- identify relevant columns (CMS occasionally renames them) ---
    col_map: dict[str, str] = {}
    for col in df.columns:
        c = col.lower()
        if "facility_name" in c or c == "facility_name":
            col_map[col] = "facility_name"
        elif c == "state":
            col_map[col] = "state"
        elif "hospital_type" in c:
            col_map[col] = "hospital_type"
        elif "emergency_services" in c:
            col_map[col] = "emergency_services"
        elif "hospital_overall_rating" in c:
            col_map[col] = "rating_raw"
        elif c in ("address", "address1"):
            col_map[col] = "address"
        elif c == "city":
            col_map[col] = "city"
        elif "zip_code" in c or c == "zip":
            col_map[col] = "zip_code"
        elif "lat" in c and "location" not in c:
            col_map[col] = "latitude"
        elif "lon" in c and "location" not in c:
            col_map[col] = "longitude"

    df = df.rename(columns=col_map)

    # Ensure required columns exist
    for required in ("facility_name", "state"):
        if required not in df.columns:
            msg = f"CMS CSV missing expected column: {required}"
            raise ValueError(msg)

    # Filter to target states
    df = df[df["state"].str.strip().isin(TARGET_STATES)].copy()
    print(f"  Hospitals in target states: {len(df)}")

    # Parse rating: coerce to float, NaN for "Not Available" / missing
    if "rating_raw" in df.columns:
        df["rating"] = pd.to_numeric(
            df["rating_raw"].replace(NOT_AVAILABLE, np.nan), errors="coerce"
        )
    else:
        df["rating"] = np.nan

    # Parse lat/lon
    for geo_col in ("latitude", "longitude"):
        if geo_col in df.columns:
            df[geo_col] = pd.to_numeric(df[geo_col], errors="coerce")
        else:
            df[geo_col] = np.nan

    # Ensure zip_code present
    if "zip_code" not in df.columns:
        df["zip_code"] = ""
    df["zip_code"] = df["zip_code"].fillna("").str.strip().str[:5]

    # Emergency services flag
    if "emergency_services" in df.columns:
        df["has_er"] = df["emergency_services"].str.strip().str.upper() == "YES"
    else:
        df["has_er"] = False

    return df[
        [
            "facility_name",
            "state",
            "hospital_type",
            "has_er",
            "rating",
            "address",
            "city",
            "zip_code",
            "latitude",
            "longitude",
        ]
    ]


# ---------------------------------------------------------------------------
# ZIP-code centroid geocoding (fallback for hospitals without lat/lon)
# ---------------------------------------------------------------------------


def geocode_zip(zip_code: str) -> tuple[float, float] | None:
    """Return (lat, lon) for a 5-digit ZIP code using Zippopotam API.

    Results are cached per ZIP to avoid repeat calls.
    """
    if not zip_code or len(zip_code) != 5:
        return None

    cached = load_cached("cms_hospitals", f"zip_{zip_code}")
    if cached is not None:
        entry = cached
        if entry.get("lat") is not None:
            return float(entry["lat"]), float(entry["lon"])
        return None

    try:
        resp = api_get(
            ZIPPOPOTAM_URL.format(zip=zip_code),
            rate_limit=0.2,
            max_retries=2,
            timeout=10,
        )
        data = resp.json()
        places = data.get("places", [])
        if places:
            lat = float(places[0]["latitude"])
            lon = float(places[0]["longitude"])
            save_cache("cms_hospitals", f"zip_{zip_code}", {"lat": lat, "lon": lon})
            return lat, lon
    except Exception:  # noqa: BLE001
        pass

    save_cache("cms_hospitals", f"zip_{zip_code}", {"lat": None, "lon": None})
    return None


def fill_missing_coordinates(hospitals: pd.DataFrame) -> pd.DataFrame:
    """Geocode hospitals that are missing lat/lon using ZIP centroids."""
    missing_mask = hospitals["latitude"].isna() | hospitals["longitude"].isna()
    missing_df = hospitals[missing_mask]

    if missing_df.empty:
        print("  All hospitals already have coordinates.")
        return hospitals

    unique_zips = missing_df["zip_code"].dropna().unique()
    print(
        f"  Geocoding {len(unique_zips)} unique ZIPs for {missing_mask.sum()} "
        "hospitals missing coordinates …"
    )

    zip_coords: dict[str, tuple[float, float] | None] = {}
    for i, z in enumerate(unique_zips):
        zip_coords[z] = geocode_zip(z)
        if (i + 1) % 50 == 0:
            print(f"    Geocoded {i + 1}/{len(unique_zips)} ZIPs …")
        time.sleep(0.0)  # rate_limit handled inside geocode_zip

    hospitals = hospitals.copy()
    for idx in hospitals[missing_mask].index:
        z = hospitals.at[idx, "zip_code"]
        coords = zip_coords.get(z)
        if coords:
            hospitals.at[idx, "latitude"] = coords[0]
            hospitals.at[idx, "longitude"] = coords[1]

    still_missing = hospitals["latitude"].isna().sum()
    print(f"  Hospitals still without coordinates (will be skipped): {still_missing}")
    return hospitals


# ---------------------------------------------------------------------------
# Distance metrics per community
# ---------------------------------------------------------------------------


def compute_metrics(
    community: pd.Series,
    hospitals: pd.DataFrame,
) -> dict[str, object]:
    """Compute all hospital access metrics for one community row."""
    clat: float = community["latitude"]
    clon: float = community["longitude"]

    # Only use hospitals with valid coordinates
    valid = hospitals.dropna(subset=["latitude", "longitude"])

    if valid.empty:
        return {
            "canonical_id": community["canonical_id"],
            "nearest_hospital_name": None,
            "nearest_hospital_miles": None,
            "nearest_hospital_rating": None,
            "nearest_er_miles": None,
            "hospitals_within_15mi": 0,
            "hospitals_within_30mi": 0,
            "avg_rating_within_30mi": None,
        }

    # Vectorised haversine for all hospitals
    distances = np.array(
        [
            haversine_miles(clat, clon, float(row["latitude"]), float(row["longitude"]))
            for _, row in valid.iterrows()
        ]
    )

    # --- Nearest hospital (any type) ---
    nearest_idx = int(np.argmin(distances))
    nearest_row = valid.iloc[nearest_idx]
    nearest_miles = float(distances[nearest_idx])
    nearest_name = str(nearest_row["facility_name"])
    nearest_rating = (
        float(nearest_row["rating"]) if pd.notna(nearest_row["rating"]) else None
    )

    # --- Nearest ER ---
    er_mask = valid["has_er"].to_numpy(dtype=bool)
    if er_mask.any():
        er_distances = distances[er_mask]
        nearest_er_miles: float | None = float(np.min(er_distances))
    else:
        nearest_er_miles = None

    # --- Counts within thresholds ---
    within_15 = int(np.sum(distances <= NEAR_MI))
    within_30 = int(np.sum(distances <= FAR_MI))

    # --- Average rating within 30 miles ---
    within_30_mask = distances <= FAR_MI
    ratings_nearby = valid.loc[within_30_mask, "rating"].dropna()
    avg_rating: float | None = (
        float(ratings_nearby.mean()) if not ratings_nearby.empty else None
    )

    return {
        "canonical_id": community["canonical_id"],
        "nearest_hospital_name": nearest_name,
        "nearest_hospital_miles": round(nearest_miles, 2),
        "nearest_hospital_rating": (
            round(nearest_rating, 1) if nearest_rating is not None else None
        ),
        "nearest_er_miles": (
            round(nearest_er_miles, 2) if nearest_er_miles is not None else None
        ),
        "hospitals_within_15mi": within_15,
        "hospitals_within_30mi": within_30,
        "avg_rating_within_30mi": (
            round(avg_rating, 2) if avg_rating is not None else None
        ),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Fetch CMS hospital data and compute access metrics for all communities."""
    print("=" * 60)
    print("07a_cms_hospitals.py – CMS Hospital Compare")
    print("=" * 60)

    ensure_dirs()

    # [1/6] Load communities
    print("\n[1/6] Loading communities …")
    communities = load_communities()
    print(f"  Communities: {len(communities)}")

    # [2/6] Download hospital data
    print("\n[2/6] Downloading CMS hospital data …")
    raw_hospitals = download_hospital_csv()
    hospitals = parse_hospitals(raw_hospitals)
    print(f"  Parsed hospitals in target states: {len(hospitals)}")

    # [3/6] Fill missing coordinates via ZIP geocoding
    print("\n[3/6] Resolving missing hospital coordinates …")
    hospitals = fill_missing_coordinates(hospitals)
    hospitals_with_coords = hospitals.dropna(subset=["latitude", "longitude"])
    print(f"  Hospitals with valid coordinates: {len(hospitals_with_coords)}")

    # [4/6] Compute metrics for each community
    print("\n[4/6] Computing hospital access metrics for each community …")
    records: list[dict[str, object]] = []
    total = len(communities)
    for i, (_, community) in enumerate(communities.iterrows()):
        rec = compute_metrics(community, hospitals_with_coords)
        records.append(rec)
        if (i + 1) % 100 == 0 or (i + 1) == total:
            print(f"  Processed {i + 1}/{total} communities …")

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS)

    # [5/6] Save CSV
    print("\n[5/6] Saving CSV …")
    csv_path = TIER1_DIR / "cms_hospitals.csv"
    result.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # [6/6] Upload to S3 and register Athena
    print("\n[6/6] Uploading to S3 and registering Athena table …")
    s3_uri = upload_to_s3(csv_path, "cms_hospitals")
    register_athena_table("tier1_cms_hospitals", ATHENA_COLUMNS, s3_uri)

    # Validation summary
    print("\n--- Validation ---")
    print(f"  Total rows: {len(result)}")

    coverage = result["nearest_hospital_miles"].notna().sum()
    print(
        f"  Communities with hospital data: {coverage}/{len(result)} "
        f"({coverage / len(result) * 100:.1f}%)"
    )

    avg_nearest = result["nearest_hospital_miles"].mean()
    avg_er = result["nearest_er_miles"].mean()
    avg_count_30 = result["hospitals_within_30mi"].mean()
    avg_rating = result["avg_rating_within_30mi"].mean()

    print(f"  Avg nearest hospital distance: {avg_nearest:.2f} miles")
    print(f"  Avg nearest ER distance:       {avg_er:.2f} miles")
    print(f"  Avg hospitals within 30mi:     {avg_count_30:.1f}")
    print(
        f"  Avg rating within 30mi:        {avg_rating:.2f}"
        if pd.notna(avg_rating)
        else "  Avg rating within 30mi:        N/A"
    )

    no_er = result["nearest_er_miles"].isna().sum()
    if no_er:
        print(f"  Communities with no ER found: {no_er}")

    print("\nDone.")


if __name__ == "__main__":
    main()
