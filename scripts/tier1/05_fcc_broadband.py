"""FCC Broadband Availability data for all MTH community counties.

Downloads FCC Form 477 county-level tier data (bulk CSV) which contains
residential fixed Internet connections per 1,000 households by county
and speed tier.  From this we derive:

  - pct_broadband_25_3:  % of households with >= 25/3 Mbps service
  - pct_broadband_100_20: % of households with >= 100/20 Mbps service
  - num_providers:  count of distinct providers in the county
  - max_download_mbps:  highest advertised download speed in the county

We also download the Form 477 county-level connection data to get
provider counts.

Data covers through December 2023 (latest available from FCC).

Usage:
    poetry run python scripts/tier1/05_fcc_broadband.py
"""

from __future__ import annotations

import sys
import time
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    CACHE_DIR,
    STATE_FIPS,
    TIER1_DIR,
    ensure_dirs,
    load_crosswalk,
    register_athena_table,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TIER_URL = "https://www.fcc.gov/sites/default/files/county_tiers_201406_202312.zip"
CONN_URL = (
    "https://www.fcc.gov/sites/default/files/county_connections_200906_202312.zip"
)

TARGET_STATES = sorted(STATE_FIPS.values())  # ["01", "12", "13"]

# The tier CSV reports connections per 1,000 households at various speed
# thresholds.  We want the December 2023 snapshot (latest period).
TARGET_PERIOD = "202312"

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "pct_broadband_25_3",
    "pct_broadband_100_20",
    "num_providers",
    "max_download_mbps",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("pct_broadband_25_3", "double"),
    ("pct_broadband_100_20", "double"),
    ("num_providers", "int"),
    ("max_download_mbps", "double"),
]

BROADBAND_COLS = [
    "pct_broadband_25_3",
    "pct_broadband_100_20",
    "num_providers",
    "max_download_mbps",
]

FCC_CACHE = CACHE_DIR / "fcc"


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------
def _build_session() -> requests.Session:
    """Build a requests session with retries and a browser-like User-Agent."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }
    )
    return session


def _download_zip(url: str, dest: Path, max_attempts: int = 3) -> None:
    """Download a ZIP file with streaming progress. Skips if cached."""
    if dest.exists():
        mb = dest.stat().st_size / (1024 * 1024)
        print(f"  ZIP already cached ({mb:.1f} MB): {dest.name}")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    session = _build_session()
    partial = dest.with_suffix(".partial")

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"  Downloading {url} (attempt {attempt}/{max_attempts}) ...")
            resp = session.get(
                url,
                stream=True,
                timeout=(30, 600),
            )
            resp.raise_for_status()

            total = 0
            with open(partial, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    mb = total / (1024 * 1024)
                    if int(mb) % 10 == 0 and int(mb) > 0:
                        print(f"    {mb:.0f} MB ...")

            partial.rename(dest)
            mb = dest.stat().st_size / (1024 * 1024)
            print(f"  Download complete: {mb:.1f} MB")
            return

        except (requests.exceptions.RequestException, OSError) as exc:
            if partial.exists():
                partial.unlink()
            if attempt < max_attempts:
                wait = 2**attempt
                print(f"  Download failed: {exc}")
                print(f"  Retrying in {wait}s ...")
                time.sleep(wait)
            else:
                raise


def _read_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    """Read the first CSV inside a ZIP into a DataFrame."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            msg = f"No CSV found in {zip_path}"
            raise FileNotFoundError(msg)
        name = csv_names[0]
        print(f"  Reading: {name}")
        with zf.open(name) as f:
            return pd.read_csv(BytesIO(f.read()), low_memory=False)


# ---------------------------------------------------------------------------
# Tier data processing
# ---------------------------------------------------------------------------
def process_tier_data(df: pd.DataFrame, target_fips: set[str]) -> pd.DataFrame:
    """Extract broadband penetration metrics from Form 477 tier data.

    The tier CSV has columns like:
      id, geography_type, state_fips, county_fips, period,
      ... and per_1000_hh columns at various speed tiers.

    We filter to the latest period, our target counties, and compute
    the percentage of households at the 25/3 and 100/20 thresholds.
    """
    print(f"  Raw tier rows: {len(df):,}")
    print(f"  Columns: {list(df.columns[:15])} ...")

    # Normalise column names
    df.columns = df.columns.str.strip().str.lower()

    # Build a 5-digit FIPS from whatever columns are available
    df = _ensure_fips(df)
    if df.empty:
        cols = ["fips_5digit", "pct_broadband_25_3", "pct_broadband_100_20"]
        return pd.DataFrame(columns=cols)

    # Filter to our states/counties and latest period
    df = df[df["fips_5digit"].isin(target_fips)].copy()
    print(f"  After FIPS filter: {len(df):,}")

    if "period" in df.columns:
        df["period"] = df["period"].astype(str).str.strip()
        available = sorted(df["period"].unique())
        best = TARGET_PERIOD if TARGET_PERIOD in available else available[-1]
        df = df[df["period"] == best].copy()
        print(f"  Using period: {best} ({len(df):,} rows)")

    # Look for speed-tier columns — the FCC uses various naming conventions
    tier_cols = _find_tier_columns(df)
    if not tier_cols:
        print("  WARNING: Could not identify speed-tier columns.")
        cols = ["fips_5digit", "pct_broadband_25_3", "pct_broadband_100_20"]
        return pd.DataFrame(columns=cols)

    result_rows: list[dict] = []
    for fips, grp in df.groupby("fips_5digit"):
        row: dict[str, object] = {"fips_5digit": fips}
        row["pct_broadband_25_3"] = _pct_at_threshold(grp, tier_cols, 25, 3)
        row["pct_broadband_100_20"] = _pct_at_threshold(grp, tier_cols, 100, 20)
        result_rows.append(row)

    return pd.DataFrame(result_rows)


def _ensure_fips(df: pd.DataFrame) -> pd.DataFrame:
    """Build a fips_5digit column from available FIPS-like columns."""
    if "fips_5digit" in df.columns:
        df["fips_5digit"] = df["fips_5digit"].astype(str).str.zfill(5)
        return df

    # Try state_fips + county_fips
    if "state_fips" in df.columns and "county_fips" in df.columns:
        df["fips_5digit"] = df["state_fips"].astype(str).str.zfill(2) + df[
            "county_fips"
        ].astype(str).str.zfill(3)
        return df

    # Try a single "fips" or "county_code" column
    for col in ("fips", "county_code", "geoid", "geo_id"):
        if col in df.columns:
            df["fips_5digit"] = df[col].astype(str).str.zfill(5)
            return df

    # Try id column that looks like a FIPS
    if "id" in df.columns:
        sample = df["id"].astype(str).iloc[0]
        if sample.isdigit() and len(sample) <= 5:
            df["fips_5digit"] = df["id"].astype(str).str.zfill(5)
            return df

    print("  WARNING: No FIPS column found in tier data.")
    return df


def _find_tier_columns(df: pd.DataFrame) -> list[tuple[str, float, float]]:
    """Identify speed-tier columns and their down/up thresholds.

    Returns list of (column_name, download_mbps, upload_mbps).
    """
    tiers: list[tuple[str, float, float]] = []
    for col in df.columns:
        c = col.lower()
        # Pattern: "at_least_25_3" or "over_25_3" or "dl25_ul3"
        # or "per_1000_25_3" etc.
        for down, up in [
            (0.2, 0.2),
            (4, 1),
            (10, 1),
            (25, 3),
            (50, 5),
            (100, 10),
            (100, 20),
            (250, 25),
            (1000, 100),
        ]:
            down_s = str(int(down)) if down == int(down) else str(down)
            up_s = str(int(up)) if up == int(up) else str(up)
            patterns = [
                f"{down_s}_{up_s}",
                f"dl{down_s}_ul{up_s}",
                f"{down_s}mbps_{up_s}mbps",
            ]
            if any(p in c for p in patterns):
                tiers.append((col, down, up))
                break
    return tiers


def _pct_at_threshold(
    grp: pd.DataFrame,
    tier_cols: list[tuple[str, float, float]],
    min_down: float,
    min_up: float,
) -> float | None:
    """Get the per-1000-households value at or above a speed threshold.

    The tier data reports connections per 1,000 households, so we
    divide by 10 to get a percentage.
    """
    matching = [(col, d, u) for col, d, u in tier_cols if d >= min_down and u >= min_up]
    if not matching:
        return None

    # Use the tier closest to the target threshold
    matching.sort(key=lambda x: (x[1], x[2]))
    col = matching[0][0]
    val = pd.to_numeric(grp[col], errors="coerce").max()
    if pd.isna(val):
        return None
    return min(round(float(val) / 10.0, 1), 100.0)


# ---------------------------------------------------------------------------
# Connection data processing (for provider counts)
# ---------------------------------------------------------------------------
def process_connection_data(df: pd.DataFrame, target_fips: set[str]) -> pd.DataFrame:
    """Extract provider count and max download speed from connection data."""
    print(f"  Raw connection rows: {len(df):,}")

    df.columns = df.columns.str.strip().str.lower()
    df = _ensure_fips(df)
    if df.empty:
        cols = ["fips_5digit", "num_providers", "max_download_mbps"]
        return pd.DataFrame(columns=cols)

    df = df[df["fips_5digit"].isin(target_fips)].copy()
    print(f"  After FIPS filter: {len(df):,}")

    if "period" in df.columns:
        df["period"] = df["period"].astype(str).str.strip()
        available = sorted(df["period"].unique())
        best = TARGET_PERIOD if TARGET_PERIOD in available else available[-1]
        df = df[df["period"] == best].copy()
        print(f"  Using period: {best} ({len(df):,} rows)")

    # Provider count: look for a providers/num_providers column
    prov_col = None
    for c in df.columns:
        if "provider" in c.lower() and ("num" in c.lower() or "count" in c.lower()):
            prov_col = c
            break

    # Max download: look for max_download or similar
    dl_col = None
    for c in df.columns:
        cl = c.lower()
        if ("max" in cl and "down" in cl) or cl == "max_advertised_downstream":
            dl_col = c
            break

    # If we can't find specific columns, try to derive from the data
    result_rows: list[dict] = []
    for fips, grp in df.groupby("fips_5digit"):
        row: dict[str, object] = {"fips_5digit": fips}
        if prov_col:
            row["num_providers"] = pd.to_numeric(grp[prov_col], errors="coerce").max()
        else:
            row["num_providers"] = None
        if dl_col:
            row["max_download_mbps"] = pd.to_numeric(grp[dl_col], errors="coerce").max()
        else:
            row["max_download_mbps"] = None
        result_rows.append(row)

    return pd.DataFrame(result_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Download, process, and store FCC broadband data for AL/FL/GA."""
    print("=" * 60)
    print("05_fcc_broadband.py - FCC Broadband Availability (Bulk Download)")
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
    # 2. Download FCC Form 477 bulk ZIPs
    # ------------------------------------------------------------------
    print("\n[2/5] Downloading FCC Form 477 bulk data ...")
    tier_zip = FCC_CACHE / "county_tiers_201406_202312.zip"
    conn_zip = FCC_CACHE / "county_connections_200906_202312.zip"

    _download_zip(TIER_URL, tier_zip)
    _download_zip(CONN_URL, conn_zip)

    # ------------------------------------------------------------------
    # 3. Process tier data (speed penetration)
    # ------------------------------------------------------------------
    print("\n[3/5] Processing tier data (broadband penetration by speed) ...")
    tier_df = _read_csv_from_zip(tier_zip)
    tier_result = process_tier_data(tier_df, target_fips)
    del tier_df  # free memory
    print(f"  Tier result rows: {len(tier_result)}")

    # ------------------------------------------------------------------
    # 4. Process connection data (providers, max speed)
    # ------------------------------------------------------------------
    print("\n[4/5] Processing connection data (providers, max speed) ...")
    conn_df = _read_csv_from_zip(conn_zip)
    conn_result = process_connection_data(conn_df, target_fips)
    del conn_df
    print(f"  Connection result rows: {len(conn_result)}")

    # Merge tier + connection results
    if not tier_result.empty and not conn_result.empty:
        broadband_df = tier_result.merge(conn_result, on="fips_5digit", how="outer")
    elif not tier_result.empty:
        broadband_df = tier_result
        broadband_df["num_providers"] = None
        broadband_df["max_download_mbps"] = None
    elif not conn_result.empty:
        broadband_df = conn_result
        broadband_df["pct_broadband_25_3"] = None
        broadband_df["pct_broadband_100_20"] = None
    else:
        print("  WARNING: No broadband data from either source.")
        broadband_df = pd.DataFrame(
            {
                "fips_5digit": sorted(target_fips),
                "pct_broadband_25_3": None,
                "pct_broadband_100_20": None,
                "num_providers": None,
                "max_download_mbps": None,
            }
        )

    # Join crosswalk -> canonical_id
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        broadband_df,
        on="fips_5digit",
        how="left",
    )

    output = merged[OUTPUT_COLUMNS].copy()
    print(f"  Output rows: {len(output)}")

    csv_path = TIER1_DIR / "fcc_broadband.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 5. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[5/5] Uploading to S3 and registering Athena table ...")
    s3_uri = upload_to_s3(csv_path, "fcc_broadband")
    register_athena_table("tier1_fcc_broadband", ATHENA_COLUMNS, s3_uri)

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    for col in BROADBAND_COLS:
        non_null = output[col].notna().sum()
        pct = non_null / total * 100 if total > 0 else 0.0
        print(f"  {col}: {non_null}/{total} non-null ({pct:.1f}%)")

    has_any = output[BROADBAND_COLS].notna().any(axis=1).sum()
    has_any_pct = has_any / total * 100 if total > 0 else 0.0
    print(
        f"  Communities with any broadband data: {has_any}/{total} ({has_any_pct:.1f}%)"
    )

    if has_any == 0:
        print(
            "\n  NOTE: Zero broadband data was retrieved. The FCC bulk data\n"
            "  format may have changed. Check the CSV column names above\n"
            "  and update the parsing logic if needed."
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
