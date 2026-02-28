"""BLS QCEW Annual Employment data for all MTH community counties.

Downloads the 2024 Quarterly Census of Employment and Wages (QCEW)
annual single-file CSV (~300 MB ZIP) from the BLS bulk download site.
Filters to total, all-industries, all-ownerships rows for each county
FIPS in our crosswalk, then extracts employment, establishment, and
wage metrics.

Usage:
    poetry run python scripts/tier1/04_bls_employment.py
"""

from __future__ import annotations

import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    CACHE_DIR,
    TIER1_DIR,
    ensure_dirs,
    load_crosswalk,
    register_athena_table,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOWNLOAD_URL = "https://data.bls.gov/cew/data/files/2024/csv/2024_annual_singlefile.zip"

# Filter values for the QCEW data
OWN_CODE = "0"  # All ownership types
INDUSTRY_CODE = "10"  # Total, all industries
SIZE_CODE = "0"  # All establishment sizes

# Column mapping: QCEW source name -> output name
COLUMN_MAP: dict[str, str] = {
    "area_fips": "fips_5digit",
    "annual_avg_emplvl": "annual_avg_employment",
    "annual_avg_estabs": "annual_avg_establishments",
    "annual_avg_wkly_wage": "avg_weekly_wage",
    "avg_annual_pay": "avg_annual_salary",
    "total_annual_wages": "total_annual_wages",
}

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "annual_avg_employment",
    "annual_avg_establishments",
    "avg_weekly_wage",
    "avg_annual_salary",
    "total_annual_wages",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("annual_avg_employment", "bigint"),
    ("annual_avg_establishments", "bigint"),
    ("avg_weekly_wage", "int"),
    ("avg_annual_salary", "int"),
    ("total_annual_wages", "bigint"),
]

ZIP_CACHE_DIR = CACHE_DIR / "bls"


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------
def download_zip(dest_path: Path) -> None:
    """Download the QCEW annual single-file ZIP with streaming.

    Skips download if the file already exists on disk.
    """
    if dest_path.exists():
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        print(f"  ZIP already cached ({size_mb:.1f} MB): {dest_path}")
        return

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  Downloading {DOWNLOAD_URL} ...")
    print("  (This is ~300 MB and may take a few minutes)")

    resp = requests.get(DOWNLOAD_URL, stream=True, timeout=300)
    resp.raise_for_status()

    total_bytes = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            total_bytes += len(chunk)
            mb = total_bytes / (1024 * 1024)
            if int(mb) % 50 == 0 and int(mb) > 0:
                print(f"    Downloaded {mb:.0f} MB ...")

    size_mb = dest_path.stat().st_size / (1024 * 1024)
    print(f"  Download complete: {size_mb:.1f} MB")


def read_csv_from_zip(zip_path: Path, target_fips: set[str]) -> pd.DataFrame:
    """Read the QCEW CSV directly from the ZIP and filter in-memory.

    Reads the CSV from the ZIP without extracting to disk, applies
    QCEW filters (own_code, industry_code, size_code), and restricts
    to the FIPS codes present in our crosswalk.
    """
    print("  Reading CSV from ZIP (in-memory) ...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        # The ZIP typically contains one CSV file
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            msg = f"No CSV found in {zip_path}"
            raise FileNotFoundError(msg)
        csv_name = csv_names[0]
        print(f"  Extracting: {csv_name}")

        with zf.open(csv_name) as csv_file:
            # Read with low_memory=False to avoid mixed-type warnings
            df = pd.read_csv(
                BytesIO(csv_file.read()),
                dtype={"area_fips": str, "own_code": str, "size_code": str},
                low_memory=False,
            )

    print(f"  Raw rows: {len(df):,}")

    # Convert industry_code to string for comparison
    df["industry_code"] = df["industry_code"].astype(str).str.strip()
    df["own_code"] = df["own_code"].astype(str).str.strip()
    df["size_code"] = df["size_code"].astype(str).str.strip()

    # Apply QCEW filters
    mask = (
        (df["own_code"] == OWN_CODE)
        & (df["industry_code"] == INDUSTRY_CODE)
        & (df["size_code"] == SIZE_CODE)
    )
    filtered = df.loc[mask].copy()
    print(f"  After QCEW filters (own=0, ind=10, size=0): {len(filtered):,}")

    # Restrict to our FIPS codes
    filtered = filtered.loc[filtered["area_fips"].isin(target_fips)].copy()
    print(f"  After FIPS filter: {len(filtered)}")

    return filtered


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Download, filter, and store BLS QCEW employment data."""
    print("=" * 60)
    print("04_bls_employment.py - BLS QCEW Annual Employment 2024")
    print("=" * 60)

    ensure_dirs()

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/5] Loading county FIPS crosswalk ...")
    crosswalk = load_crosswalk()
    target_fips = set(crosswalk["fips_5digit"].unique())
    print(f"  Crosswalk rows: {len(crosswalk)}")
    print(f"  Unique target FIPS: {len(target_fips)}")

    # ------------------------------------------------------------------
    # 2. Download ZIP
    # ------------------------------------------------------------------
    print("\n[2/5] Downloading QCEW annual single-file ZIP ...")
    zip_path = ZIP_CACHE_DIR / "2024_annual_singlefile.zip"
    download_zip(zip_path)

    # ------------------------------------------------------------------
    # 3. Read and filter CSV from ZIP
    # ------------------------------------------------------------------
    print("\n[3/5] Reading and filtering QCEW data ...")
    qcew_df = read_csv_from_zip(zip_path, target_fips)

    # Rename columns to output names
    source_cols = list(COLUMN_MAP.keys())
    qcew_df = qcew_df[source_cols].rename(columns=COLUMN_MAP).copy()

    # Coerce numeric columns
    numeric_cols = [
        "annual_avg_employment",
        "annual_avg_establishments",
        "avg_weekly_wage",
        "avg_annual_salary",
        "total_annual_wages",
    ]
    for col in numeric_cols:
        qcew_df[col] = pd.to_numeric(qcew_df[col], errors="coerce")

    print(f"  QCEW rows for our counties: {len(qcew_df)}")

    # ------------------------------------------------------------------
    # 4. Join crosswalk -> canonical_id
    # ------------------------------------------------------------------
    print("\n[4/5] Joining with crosswalk ...")
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        qcew_df,
        on="fips_5digit",
        how="left",
    )

    output = merged[OUTPUT_COLUMNS].copy()
    print(f"  Output rows: {len(output)}")

    # Save CSV
    csv_path = TIER1_DIR / "bls_employment.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 5. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[5/5] Uploading to S3 and registering Athena table ...")
    s3_uri = upload_to_s3(csv_path, "bls_employment")
    register_athena_table("tier1_bls_employment", ATHENA_COLUMNS, s3_uri)

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    coverage = output["annual_avg_employment"].notna().sum()
    coverage_pct = coverage / total * 100 if total > 0 else 0.0
    print(f"  Total rows: {total}")
    print(f"  Coverage (non-null employment): {coverage}/{total} ({coverage_pct:.1f}%)")

    salary_valid = output["avg_annual_salary"].dropna()
    if not salary_valid.empty:
        print(
            f"  Salary range: ${salary_valid.min():,.0f} - ${salary_valid.max():,.0f}"
        )
        print(f"  Median salary: ${salary_valid.median():,.0f}")
    else:
        print("  Salary range: N/A (no data)")

    emp_valid = output["annual_avg_employment"].dropna()
    if not emp_valid.empty:
        print(f"  Employment range: {emp_valid.min():,.0f} - {emp_valid.max():,.0f}")

    null_counts = output[numeric_cols].isna().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        print("  Columns with nulls:")
        for col, cnt in cols_with_nulls.items():
            print(f"    {col}: {cnt} null(s)")
    else:
        print("  No null values in numeric columns.")

    print("\nDone.")


if __name__ == "__main__":
    main()
