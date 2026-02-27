"""Fetch NOAA Climate Normals (1991-2020) for all MTH communities.

Phase 1: Find the nearest NOAA NORMAL_ANN station for each community using
         a bounding-box search, expanding from 0.5° to 1.0° if needed.
Phase 2: Fetch annual climate data for each unique station (deduplicated).

Output: data/prepared/tier1/noaa_climate.csv
Columns: canonical_id, noaa_station_id, station_distance_miles,
         ann_tavg_f, ann_tmax_f, ann_tmin_f, ann_prcp_in,
         ann_snow_in, ann_htdd, ann_cldd
"""

from __future__ import annotations

import sys
from pathlib import Path

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
    require_env,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
DATASET_ID = "NORMAL_ANN"
DATA_TYPES = (
    "ANN-TAVG-NORMAL,"
    "ANN-TMAX-NORMAL,"
    "ANN-TMIN-NORMAL,"
    "ANN-PRCP-NORMAL,"
    "ANN-SNOW-NORMAL,"
    "ANN-HTDD-NORMAL,"
    "ANN-CLDD-NORMAL"
)
RATE_LIMIT = 0.2  # 5 req/sec
OUTPUT_FILENAME = "noaa_climate.csv"

# Mapping from NOAA datatype → (output column, divisor)
DATATYPE_MAP: dict[str, tuple[str, float]] = {
    "ANN-TAVG-NORMAL": ("ann_tavg_f", 10.0),
    "ANN-TMAX-NORMAL": ("ann_tmax_f", 10.0),
    "ANN-TMIN-NORMAL": ("ann_tmin_f", 10.0),
    "ANN-PRCP-NORMAL": ("ann_prcp_in", 100.0),
    "ANN-SNOW-NORMAL": ("ann_snow_in", 10.0),
    "ANN-HTDD-NORMAL": ("ann_htdd", 1.0),
    "ANN-CLDD-NORMAL": ("ann_cldd", 1.0),
}

# Athena schema (canonical_id and noaa_station_id as string, rest as double)
ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("noaa_station_id", "string"),
    ("station_distance_miles", "double"),
    ("ann_tavg_f", "double"),
    ("ann_tmax_f", "double"),
    ("ann_tmin_f", "double"),
    ("ann_prcp_in", "double"),
    ("ann_snow_in", "double"),
    ("ann_htdd", "double"),
    ("ann_cldd", "double"),
]


# ---------------------------------------------------------------------------
# Phase 1: station lookup
# ---------------------------------------------------------------------------
def find_nearest_station(
    canonical_id: str,
    lat: float,
    lng: float,
    headers: dict[str, str],
) -> tuple[str | None, float | None]:
    """Return (station_id, distance_miles) for the nearest NORMAL_ANN station.

    Tries a 0.5° bounding box first, expands to 1.0° if no results.
    Returns (None, None) if no station found at either radius.
    """
    for half_deg in (0.5, 1.0):
        cache_key = f"stations_{canonical_id}_{half_deg}"
        cached = load_cached("noaa", cache_key)
        if cached is None:
            extent = (
                f"{lat - half_deg},{lng - half_deg},{lat + half_deg},{lng + half_deg}"
            )
            params = {
                "datasetid": DATASET_ID,
                "extent": extent,
                "limit": 25,
            }
            try:
                resp = api_get(
                    f"{BASE_URL}/stations",
                    params=params,
                    headers=headers,
                    rate_limit=RATE_LIMIT,
                )
                payload = resp.json()
            except Exception as exc:
                print(f"  Warning: station lookup failed for {canonical_id}: {exc}")
                payload = {}
            save_cache("noaa", cache_key, payload)
        else:
            payload = cached

        results = payload.get("results", [])
        if not results:
            continue  # expand bbox

        # Pick closest station by haversine
        best_id: str | None = None
        best_dist = float("inf")
        for station in results:
            s_lat = station.get("latitude")
            s_lng = station.get("longitude")
            if s_lat is None or s_lng is None:
                continue
            dist = haversine_miles(lat, lng, float(s_lat), float(s_lng))
            if dist < best_dist:
                best_dist = dist
                best_id = station["id"]

        if best_id is not None:
            return best_id, best_dist

    return None, None


# ---------------------------------------------------------------------------
# Phase 2: fetch climate data for a station
# ---------------------------------------------------------------------------
def fetch_station_data(
    station_id: str,
    headers: dict[str, str],
) -> dict[str, float | None]:
    """Return a dict of output-column → value for one station.

    Values are scaled from NOAA tenths/hundredths to human-readable units.
    Missing datatypes are returned as None.
    """
    cache_key = f"data_{station_id}"
    cached = load_cached("noaa", cache_key)
    if cached is None:
        params = {
            "datasetid": DATASET_ID,
            "stationid": station_id,
            "datatypeid": DATA_TYPES,
            "limit": 100,
        }
        try:
            resp = api_get(
                f"{BASE_URL}/data",
                params=params,
                headers=headers,
                rate_limit=RATE_LIMIT,
            )
            payload = resp.json()
        except Exception as exc:
            print(f"  Warning: data fetch failed for {station_id}: {exc}")
            payload = {}
        save_cache("noaa", cache_key, payload)
    else:
        payload = cached

    # Build output dict initialised to None
    output: dict[str, float | None] = {col: None for col, _ in DATATYPE_MAP.values()}
    for record in payload.get("results", []):
        dtype = record.get("datatype")
        if dtype not in DATATYPE_MAP:
            continue
        col, divisor = DATATYPE_MAP[dtype]
        raw = record.get("value")
        if raw is not None:
            output[col] = float(raw) / divisor if divisor != 1.0 else float(raw)

    return output


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ensure_dirs()

    noaa_token = require_env("NOAA_API_KEY")
    headers = {"token": noaa_token}

    # ------------------------------------------------------------------
    # Load communities
    # ------------------------------------------------------------------
    communities = load_communities()
    total = len(communities)
    print(f"Loaded {total} communities.")

    # ------------------------------------------------------------------
    # Phase 1: find nearest station per community
    # ------------------------------------------------------------------
    print("\n--- Phase 1: Finding nearest NOAA station per community ---")
    station_lookup: dict[str, tuple[str | None, float | None]] = {}

    for idx, row in communities.iterrows():
        canonical_id: str = row["canonical_id"]
        lat: float = float(row["latitude"])
        lng: float = float(row["longitude"])

        station_id, distance = find_nearest_station(canonical_id, lat, lng, headers)
        station_lookup[canonical_id] = (station_id, distance)

        row_num = int(idx) + 1  # type: ignore[arg-type]
        if row_num % 100 == 0 or row_num == total:
            found = sum(1 for s, _ in station_lookup.values() if s is not None)
            print(f"  Progress: {row_num}/{total} communities ({found} stations found)")

    # ------------------------------------------------------------------
    # Phase 2: fetch data for each unique station (deduplicated)
    # ------------------------------------------------------------------
    unique_stations: set[str] = {
        sid for sid, _ in station_lookup.values() if sid is not None
    }
    print(
        f"\n--- Phase 2: Fetching climate data for "
        f"{len(unique_stations)} unique stations ---"
    )

    station_data: dict[str, dict[str, float | None]] = {}
    for i, station_id in enumerate(sorted(unique_stations), 1):
        station_data[station_id] = fetch_station_data(station_id, headers)
        if i % 100 == 0 or i == len(unique_stations):
            print(f"  Progress: {i}/{len(unique_stations)} stations fetched")

    # ------------------------------------------------------------------
    # Build output DataFrame
    # ------------------------------------------------------------------
    print("\n--- Building output DataFrame ---")
    climate_cols = [col for col, _ in DATATYPE_MAP.values()]
    rows: list[dict] = []

    for _, row in communities.iterrows():
        canonical_id = row["canonical_id"]
        station_id, distance = station_lookup.get(canonical_id, (None, None))

        record: dict = {
            "canonical_id": canonical_id,
            "noaa_station_id": station_id,
            "station_distance_miles": distance,
        }

        if station_id is not None and station_id in station_data:
            record.update(station_data[station_id])
        else:
            for col in climate_cols:
                record[col] = None

        rows.append(record)

    output_cols = [
        "canonical_id",
        "noaa_station_id",
        "station_distance_miles",
    ] + climate_cols
    df = pd.DataFrame(rows, columns=output_cols)

    # ------------------------------------------------------------------
    # Save CSV
    # ------------------------------------------------------------------
    csv_path = TIER1_DIR / OUTPUT_FILENAME
    df.to_csv(csv_path, index=False)
    print(f"\nSaved {len(df)} rows → {csv_path}")

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    station_coverage = df["noaa_station_id"].notna().sum()
    coverage_pct = station_coverage / len(df) * 100
    avg_dist = df["station_distance_miles"].dropna().mean()
    t_min = df["ann_tmin_f"].dropna().min()
    t_max = df["ann_tmax_f"].dropna().max()

    print("\n--- Validation ---")
    print(f"  Rows:               {len(df)}")
    print(f"  Station coverage:   {station_coverage}/{len(df)} ({coverage_pct:.1f}%)")
    print(f"  Avg station dist:   {avg_dist:.1f} miles")
    print(f"  Temp range (min):   {t_min:.1f}°F")
    print(f"  Temp range (max):   {t_max:.1f}°F")

    # ------------------------------------------------------------------
    # Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    s3_uri = upload_to_s3(csv_path, "noaa_climate")
    register_athena_table("tier1_noaa_climate", ATHENA_COLUMNS, s3_uri)

    print("\nDone.")


if __name__ == "__main__":
    main()
