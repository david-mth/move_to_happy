"""Move to Happy — FastAPI backend.

Initialises the LME engine once at startup, then serves scoring requests
and enriches results with tier-1 data.
"""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env", override=True)

# ---------------------------------------------------------------------------
# Make the LME package importable without installing it as a package.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from enrichment import EnrichmentStore  # noqa: E402

from move_to_happy.lme import LMEEngine, UserPreferences  # noqa: E402

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TIER1_DIR = DATA_DIR / "tier1"

DATAFRAME_NAMES: dict[str, str] = {
    "communities": "mth_communities.csv",
    "census": "tier1/census_acs.csv",
    "crime": "tier1/fbi_crime.csv",
    "broadband": "tier1/fcc_broadband.csv",
    "air_quality": "tier1/epa_air_quality.csv",
    "employment": "tier1/bls_employment.csv",
    "hospitals": "tier1/cms_hospitals.csv",
    "physicians": "tier1/cms_physicians.csv",
    "geocoder": "tier1/geocoder.csv",
    "geodistance": "tier1/geodistance.csv",
    "tax_rates": "tier1/tax_rates.csv",
    "education": "tier1/county_education.csv",
    "lake_distance": "tier1/lake_distance.csv",
}

# ---------------------------------------------------------------------------
# Global singletons (populated during lifespan startup)
# ---------------------------------------------------------------------------
engine: LMEEngine | None = None
enrichment = EnrichmentStore()
chat_dataframes: dict[str, pd.DataFrame] = {}
concierge_sessions: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global engine
    community_csv = DATA_DIR / "mth_communities.csv"
    if community_csv.exists():
        df = pd.read_csv(community_csv)
        engine = LMEEngine(df)
        enrichment.load()

        for name, rel_path in DATAFRAME_NAMES.items():
            csv_path = DATA_DIR / rel_path
            if csv_path.exists():
                chat_dataframes[name] = pd.read_csv(csv_path)
    else:
        import logging

        logging.warning(
            "Community data not found at %s. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "to sync data from S3.",
            community_csv,
        )

    yield


app = FastAPI(title="Move to Happy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------
class ScoreRequest(BaseModel):
    monthly_payment: float = 2_500.0
    loan_term_years: int = 30
    down_payment_pct: float = 0.10
    bedbath_bucket: str = "BB2"
    property_type_pref: str = "SFH"
    anchor_lat: float = 33.749
    anchor_lon: float = -84.388
    anchor_state: str = "Georgia"
    max_radius_miles: float = 120.0
    pref_mountains: float = 0.30
    pref_beach: float = 0.15
    pref_lake: float = 0.10
    pref_airport: float = 0.10
    pref_climate: float = 0.15
    pref_terrain: float = 0.10
    pref_cost: float = 0.10
    preferred_climate: str = "Temperate"
    preferred_terrain: str = "Mountains"
    top_n: int = Field(default=25, ge=1, le=100)


class ScoreResponse(BaseModel):
    rankings: list[dict[str, Any]]
    total_candidates: int
    eliminated_count: int
    max_purchase_price: int
    affordability_window: list[int]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/api/score", response_model=ScoreResponse)
async def score(req: ScoreRequest):
    assert engine is not None
    prefs = UserPreferences(
        monthly_payment=req.monthly_payment,
        loan_term_years=req.loan_term_years,
        down_payment_pct=req.down_payment_pct,
        bedbath_bucket=req.bedbath_bucket,
        property_type_pref=req.property_type_pref,
        anchor_lat=req.anchor_lat,
        anchor_lon=req.anchor_lon,
        anchor_state=req.anchor_state,
        max_radius_miles=req.max_radius_miles,
        pref_mountains=req.pref_mountains,
        pref_beach=req.pref_beach,
        pref_lake=req.pref_lake,
        pref_airport=req.pref_airport,
        pref_climate=req.pref_climate,
        pref_terrain=req.pref_terrain,
        pref_cost=req.pref_cost,
        preferred_climate=req.preferred_climate,
        preferred_terrain=req.preferred_terrain,
    )
    result = engine.score(prefs, top_n=req.top_n)
    raw = asdict(result)

    for community in raw["rankings"]:
        cid = community.get("canonical_id", "")
        community["enrichment"] = enrichment.enrich(cid)

    return ScoreResponse(
        rankings=raw["rankings"],
        total_candidates=raw["total_candidates"],
        eliminated_count=raw["eliminated_count"],
        max_purchase_price=raw["max_purchase_price"],
        affordability_window=list(raw["affordability_window"]),
    )


class ConciergeRequest(BaseModel):
    message: str
    session_id: str = "default"


class ConciergeResponse(BaseModel):
    role: str = "assistant"
    content: str
    results: dict[str, Any] | None = None
    explanations: list[dict[str, str]] | None = None
    needs_clarification: list[str] | None = None
    session_id: str = "default"


def _get_or_create_session(session_id: str) -> Any:
    """Get or create a ConciergeOrchestrator for the given session."""
    if session_id in concierge_sessions:
        return concierge_sessions[session_id]

    try:
        from move_to_happy.ai.claude_client import ClaudeClient
        from move_to_happy.ai.concierge import ConciergeOrchestrator
        from move_to_happy.ai.explanation import ExplanationGenerator
        from move_to_happy.ai.intake import IntakeInterpreter
        from move_to_happy.rag.indexer import FAISSIndex
        from move_to_happy.rag.retriever import RAGRetriever

        claude = ClaudeClient()
        intake = IntakeInterpreter(claude)

        rag_index = FAISSIndex()
        rag_dir = PROJECT_ROOT / "data" / "rag_index"
        if rag_dir.exists():
            rag_index.load(rag_dir)

        rag = RAGRetriever(index=rag_index)
        explainer = ExplanationGenerator(claude, rag)

        session = ConciergeOrchestrator(
            claude=claude,
            intake=intake,
            explainer=explainer,
            rag=rag,
            lme=engine,
        )
        concierge_sessions[session_id] = session
        return session
    except Exception:
        import logging

        logging.exception("Failed to create concierge session")
        return None


@app.post("/api/concierge/message", response_model=ConciergeResponse)
async def concierge_message(req: ConciergeRequest):
    if engine is None:
        return ConciergeResponse(
            content="The matching engine is not loaded. Please try again later.",
            session_id=req.session_id,
        )

    session = _get_or_create_session(req.session_id)
    if session is None:
        return ConciergeResponse(
            content=(
                "The AI concierge is unavailable. Make sure the "
                "ANTHROPIC_API_KEY environment variable is set."
            ),
            session_id=req.session_id,
        )

    result = session.handle_message(req.message)

    # Attach tier-1 enrichment to each ranked community (mirrors /api/score)
    raw_results = result.get("results")
    if raw_results and raw_results.get("rankings"):
        for community in raw_results["rankings"]:
            cid = community.get("canonical_id", "")
            community["enrichment"] = enrichment.enrich(cid)

    return ConciergeResponse(
        content=result.get("content", ""),
        results=raw_results,
        explanations=result.get("explanations"),
        needs_clarification=result.get("needs_clarification"),
        session_id=req.session_id,
    )


@app.get("/api/concierge/status")
async def concierge_status():
    try:
        import os

        import anthropic  # noqa: F401

        has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
        return {"available": engine is not None and has_key}
    except ImportError:
        return {"available": False}


@app.post("/api/lead-summary")
async def lead_summary(session_id: str = "default"):
    session = concierge_sessions.get(session_id)
    if session is None:
        return {"error": "No active session found"}

    try:
        from move_to_happy.ai.claude_client import ClaudeClient
        from move_to_happy.ai.lead_summary import LeadSummaryAgent

        claude = ClaudeClient()
        agent = LeadSummaryAgent(claude)
        data = session.get_session_data()
        summary = agent.generate_summary(
            conversation_history=data["conversation_history"],
            extracted_preferences=data["extracted_preferences"],
            lme_results=data["lme_results"],
        )
        return summary
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/communities/{canonical_id}")
async def community_detail(canonical_id: str):
    """Return community profile by canonical ID."""
    comm = chat_dataframes.get("communities")
    if comm is None:
        return {"error": "Community data not loaded"}
    row = comm[comm["canonical_id"] == canonical_id]
    if row.empty:
        return {"error": "Community not found"}
    record = row.iloc[0].to_dict()
    record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
    record["enrichment"] = enrichment.enrich(canonical_id)
    return record


@app.get("/api/data/list")
async def data_list():
    """Return available CSV datasets with row/column counts."""
    datasets = []
    for csv_path in sorted(DATA_DIR.rglob("*.csv")):
        rel = csv_path.relative_to(DATA_DIR)
        df = pd.read_csv(csv_path, nrows=0)
        row_count = sum(1 for _ in open(csv_path)) - 1  # noqa: SIM115
        datasets.append(
            {
                "name": str(rel).replace("\\", "/"),
                "rows": row_count,
                "columns": len(df.columns),
                "column_names": list(df.columns),
            }
        )
    return datasets


@app.get("/api/data/{name:path}")
async def data_read(name: str, offset: int = 0, limit: int = 100):
    """Return paginated rows from a CSV dataset as JSON records."""
    csv_path = (DATA_DIR / name).resolve()
    if not str(csv_path).startswith(str(DATA_DIR.resolve())):
        return {"error": "Invalid path"}
    if not csv_path.exists() or csv_path.suffix != ".csv":
        return {"error": "Dataset not found"}
    df = pd.read_csv(csv_path)
    total = len(df)
    page = df.iloc[offset : offset + limit]
    records = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in page.to_dict(orient="records")
    ]
    return {
        "name": name,
        "total_rows": total,
        "offset": offset,
        "limit": limit,
        "columns": list(df.columns),
        "rows": records,
    }


@app.get("/api/eda/columns")
async def eda_columns():
    """Return all datasets with numeric columns and basic stats for EDA."""
    result = []
    for name, df in chat_dataframes.items():
        numeric_cols = []
        for col in df.columns:
            if col in ("canonical_id", "fips_5digit"):
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                s = df[col].dropna()
                numeric_cols.append(
                    {
                        "name": col,
                        "min": round(float(s.min()), 4) if len(s) else 0,
                        "max": round(float(s.max()), 4) if len(s) else 0,
                        "mean": round(float(s.mean()), 4) if len(s) else 0,
                        "count": int(s.count()),
                    }
                )
        cat_cols = []
        for col in df.columns:
            if col in ("canonical_id", "fips_5digit"):
                continue
            if df[col].dtype == "object" and df[col].nunique() < 20:
                cat_cols.append(
                    {
                        "name": col,
                        "values": sorted(df[col].dropna().unique().tolist()),
                    }
                )
        result.append(
            {
                "name": name,
                "rows": len(df),
                "numeric_columns": numeric_cols,
                "categorical_columns": cat_cols,
            }
        )
    return result


@app.get("/api/eda/data/{dataset_name}")
async def eda_data(dataset_name: str, columns: str = "", state: str = ""):
    """Return specific columns from a dataset for EDA charts."""
    if dataset_name not in chat_dataframes:
        return {"error": "Dataset not found"}
    df = chat_dataframes[dataset_name]
    if state and state != "All" and "state_name" in df.columns:
        df = df[df["state_name"] == state]
    if columns:
        cols = [c.strip() for c in columns.split(",") if c.strip() in df.columns]
    else:
        cols = list(df.columns)
    if "canonical_id" not in cols and "canonical_id" in df.columns:
        cols = ["canonical_id"] + cols
    subset = df[cols].copy()
    records = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in subset.to_dict(orient="records")
    ]
    return {"name": dataset_name, "columns": cols, "rows": records}


def _percentile_rank(series: pd.Series, invert: bool = False) -> pd.Series:
    """Convert a series to 0-100 percentile ranks. NaN stays NaN."""
    ranked = series.rank(pct=True, na_option="keep") * 100
    if invert:
        ranked = 100 - ranked
    return ranked


def _build_merged_df() -> pd.DataFrame:
    """Merge all datasets on canonical_id for composite index computation."""
    comm = chat_dataframes.get("communities")
    if comm is None:
        return pd.DataFrame()
    base_cols = [
        "canonical_id",
        "city_state",
        "state_name",
        "latitude",
        "longitude",
        "population",
        "cost_of_living",
    ]
    for extra in ["miles_to_lake", "miles_to_beach", "miles_to_mountains"]:
        if extra in comm.columns:
            base_cols.append(extra)
    base = comm[base_cols].copy()
    join_map = {
        "census": ["median_home_value", "median_household_income"],
        "crime": ["violent_crime_rate", "property_crime_rate"],
        "hospitals": [
            "nearest_hospital_miles",
            "hospitals_within_30mi",
            "avg_rating_within_30mi",
        ],
        "physicians": ["providers_per_1000_pop"],
        "education": [
            "hs_graduation_rate",
            "postsecondary_completion_rate",
            "median_earnings",
        ],
        "broadband": ["pct_broadband_100_20", "num_providers", "max_download_mbps"],
        "air_quality": ["pm25_mean", "ozone_mean"],
        "tax_rates": [
            "effective_property_tax_rate",
            "combined_sales_tax_rate",
        ],
        "employment": ["avg_annual_salary", "annual_avg_employment"],
        "lake_distance": ["lake_distance_miles", "lake_name", "lake_area_sq_mi"],
    }
    for ds_name, cols in join_map.items():
        ds = chat_dataframes.get(ds_name)
        if ds is None:
            continue
        available = [c for c in cols if c in ds.columns]
        if not available or "canonical_id" not in ds.columns:
            continue
        base = base.merge(
            ds[["canonical_id"] + available],
            on="canonical_id",
            how="left",
        )
    return base


@app.get("/api/eda/summary")
async def eda_summary(state: str = "All"):
    """Return KPIs and composite livability indices for all communities."""
    merged = _build_merged_df()
    if merged.empty:
        return {"kpis": {}, "indices": []}

    if state and state != "All":
        filtered = merged[merged["state_name"] == state]
    else:
        filtered = merged

    def _safe_median(col: str) -> float | None:
        if col not in filtered.columns:
            return None
        v = filtered[col].dropna()
        return round(float(v.median()), 2) if len(v) else None

    def _safe_mean(col: str) -> float | None:
        if col not in filtered.columns:
            return None
        v = filtered[col].dropna()
        return round(float(v.mean()), 2) if len(v) else None

    kpis = {
        "total_communities": len(filtered),
        "median_home_value": _safe_median("median_home_value"),
        "median_income": _safe_median("median_household_income"),
        "avg_violent_crime_rate": _safe_mean("violent_crime_rate"),
        "avg_broadband_pct": _safe_mean("pct_broadband_100_20"),
        "avg_property_tax_rate": _safe_mean("effective_property_tax_rate"),
        "avg_lake_distance": _safe_mean("lake_distance_miles"),
    }

    idx = merged.copy()

    aff_cols = []
    if "cost_of_living" in idx.columns:
        idx["_aff_col"] = _percentile_rank(idx["cost_of_living"], invert=True)
        aff_cols.append("_aff_col")
    if "median_home_value" in idx.columns:
        idx["_aff_hv"] = _percentile_rank(idx["median_home_value"], invert=True)
        aff_cols.append("_aff_hv")
    if "effective_property_tax_rate" in idx.columns:
        idx["_aff_pt"] = _percentile_rank(
            idx["effective_property_tax_rate"],
            invert=True,
        )
        aff_cols.append("_aff_pt")
    if "combined_sales_tax_rate" in idx.columns:
        idx["_aff_st"] = _percentile_rank(
            idx["combined_sales_tax_rate"],
            invert=True,
        )
        aff_cols.append("_aff_st")
    idx["affordability"] = idx[aff_cols].mean(axis=1).round(1) if aff_cols else np.nan

    safety_cols = []
    if "violent_crime_rate" in idx.columns:
        idx["_saf_v"] = _percentile_rank(idx["violent_crime_rate"], invert=True)
        safety_cols.append("_saf_v")
    if "property_crime_rate" in idx.columns:
        idx["_saf_p"] = _percentile_rank(idx["property_crime_rate"], invert=True)
        safety_cols.append("_saf_p")
    idx["safety"] = idx[safety_cols].mean(axis=1).round(1) if safety_cols else np.nan

    hc_cols = []
    if "nearest_hospital_miles" in idx.columns:
        idx["_hc_dist"] = _percentile_rank(
            idx["nearest_hospital_miles"],
            invert=True,
        )
        hc_cols.append("_hc_dist")
    if "hospitals_within_30mi" in idx.columns:
        idx["_hc_cnt"] = _percentile_rank(idx["hospitals_within_30mi"])
        hc_cols.append("_hc_cnt")
    if "providers_per_1000_pop" in idx.columns:
        idx["_hc_prov"] = _percentile_rank(idx["providers_per_1000_pop"])
        hc_cols.append("_hc_prov")
    if "avg_rating_within_30mi" in idx.columns:
        idx["_hc_rat"] = _percentile_rank(idx["avg_rating_within_30mi"])
        hc_cols.append("_hc_rat")
    idx["healthcare"] = idx[hc_cols].mean(axis=1).round(1) if hc_cols else np.nan

    edu_cols = []
    if "hs_graduation_rate" in idx.columns:
        idx["_edu_hs"] = _percentile_rank(idx["hs_graduation_rate"])
        edu_cols.append("_edu_hs")
    if "postsecondary_completion_rate" in idx.columns:
        idx["_edu_ps"] = _percentile_rank(idx["postsecondary_completion_rate"])
        edu_cols.append("_edu_ps")
    if "median_earnings" in idx.columns:
        idx["_edu_earn"] = _percentile_rank(idx["median_earnings"])
        edu_cols.append("_edu_earn")
    idx["education"] = idx[edu_cols].mean(axis=1).round(1) if edu_cols else np.nan

    dig_cols = []
    if "pct_broadband_100_20" in idx.columns:
        idx["_dig_bb"] = _percentile_rank(idx["pct_broadband_100_20"])
        dig_cols.append("_dig_bb")
    if "num_providers" in idx.columns:
        idx["_dig_np"] = _percentile_rank(idx["num_providers"])
        dig_cols.append("_dig_np")
    if "max_download_mbps" in idx.columns:
        idx["_dig_dl"] = _percentile_rank(idx["max_download_mbps"])
        dig_cols.append("_dig_dl")
    idx["digital"] = idx[dig_cols].mean(axis=1).round(1) if dig_cols else np.nan

    env_cols = []
    if "pm25_mean" in idx.columns:
        idx["_env_pm"] = _percentile_rank(idx["pm25_mean"], invert=True)
        env_cols.append("_env_pm")
    if "ozone_mean" in idx.columns:
        idx["_env_oz"] = _percentile_rank(idx["ozone_mean"], invert=True)
        env_cols.append("_env_oz")
    idx["environmental"] = idx[env_cols].mean(axis=1).round(1) if env_cols else np.nan

    rec_cols = []
    if "lake_distance_miles" in idx.columns:
        idx["_rec_lake"] = _percentile_rank(
            idx["lake_distance_miles"],
            invert=True,
        )
        rec_cols.append("_rec_lake")
    if "miles_to_lake" in idx.columns:
        idx["_rec_mtl"] = _percentile_rank(idx["miles_to_lake"], invert=True)
        rec_cols.append("_rec_mtl")
    if "miles_to_beach" in idx.columns:
        idx["_rec_beach"] = _percentile_rank(idx["miles_to_beach"], invert=True)
        rec_cols.append("_rec_beach")
    if "miles_to_mountains" in idx.columns:
        idx["_rec_mtn"] = _percentile_rank(
            idx["miles_to_mountains"],
            invert=True,
        )
        rec_cols.append("_rec_mtn")
    idx["recreation"] = idx[rec_cols].mean(axis=1).round(1) if rec_cols else np.nan

    out_cols = [
        "canonical_id",
        "city_state",
        "state_name",
        "latitude",
        "longitude",
        "population",
        "affordability",
        "safety",
        "healthcare",
        "education",
        "digital",
        "environmental",
        "recreation",
    ]
    result = idx[[c for c in out_cols if c in idx.columns]].copy()
    records = [
        {
            k: (None if pd.isna(v) else round(v, 2) if isinstance(v, float) else v)
            for k, v in row.items()
        }
        for row in result.to_dict(orient="records")
    ]
    return {"kpis": kpis, "indices": records}


@app.get("/api/eda/correlations")
async def eda_correlations(columns: str = ""):
    """Return pairwise Pearson correlation matrix for selected columns."""
    merged = _build_merged_df()
    if merged.empty:
        return {"columns": [], "matrix": []}

    default_cols = [
        "population",
        "cost_of_living",
        "median_home_value",
        "median_household_income",
        "violent_crime_rate",
        "property_crime_rate",
        "pct_broadband_100_20",
        "pm25_mean",
        "avg_annual_salary",
        "lake_distance_miles",
        "nearest_hospital_miles",
        "providers_per_1000_pop",
        "hs_graduation_rate",
        "effective_property_tax_rate",
        "combined_sales_tax_rate",
    ]
    if columns:
        col_list = [c.strip() for c in columns.split(",") if c.strip()]
    else:
        col_list = default_cols

    available = [c for c in col_list if c in merged.columns]
    if len(available) < 2:
        return {"columns": available, "matrix": []}

    corr = merged[available].corr(numeric_only=True)
    matrix = []
    for row_name in corr.index:
        row_vals = {}
        for col_name in corr.columns:
            v = corr.loc[row_name, col_name]
            row_vals[col_name] = round(float(v), 4) if not pd.isna(v) else None
        matrix.append({"column": row_name, **row_vals})
    return {"columns": list(corr.columns), "matrix": matrix}


@app.get("/api/metadata")
async def metadata():
    return {
        "states": ["Georgia", "Alabama", "Florida"],
        "climates": [
            "Temperate",
            "Subtropical",
            "Tropical",
            "Arid",
            "Semi-Arid",
            "Continental",
        ],
        "terrains": [
            "Mountains",
            "Hills",
            "Piedmont",
            "Plains",
            "Coastal",
            "Swamp/Marsh",
        ],
        "bedbath_buckets": ["BB1", "BB2", "BB3"],
        "property_types": ["SFH", "Any"],
        "ranges": {
            "monthly_payment": {"min": 500, "max": 8000, "step": 100},
            "down_payment_pct": {"min": 0.03, "max": 0.50, "step": 0.01},
            "max_radius_miles": {"min": 25, "max": 300, "step": 5},
            "loan_term_years": [15, 30],
        },
    }


# ---------------------------------------------------------------------------
# Serve built frontend (production / Replit)
# ---------------------------------------------------------------------------
STATIC_DIR = Path(__file__).resolve().parent / "static"

if STATIC_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):  # noqa: ARG001
        return FileResponse(STATIC_DIR / "index.html")
