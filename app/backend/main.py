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
}

# ---------------------------------------------------------------------------
# Global singletons (populated during lifespan startup)
# ---------------------------------------------------------------------------
engine: LMEEngine | None = None
enrichment = EnrichmentStore()
chat_dataframes: dict[str, pd.DataFrame] = {}
data_chat: Any = None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global engine, data_chat
    df = pd.read_csv(DATA_DIR / "mth_communities.csv")
    engine = LMEEngine(df)
    enrichment.load()

    for name, rel_path in DATAFRAME_NAMES.items():
        csv_path = DATA_DIR / rel_path
        if csv_path.exists():
            chat_dataframes[name] = pd.read_csv(csv_path)

    try:
        from chat import DataChat  # noqa: E402

        data_chat = DataChat(chat_dataframes)
    except (ImportError, ValueError):
        data_chat = None

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


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, Any]] = Field(default_factory=list)


class ChatResponse(BaseModel):
    role: str = "assistant"
    content: str
    table: list[dict[str, str]] | None = None


@app.post("/api/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if data_chat is None:
        return ChatResponse(
            content=(
                "Chat is unavailable. Make sure the ANTHROPIC_API_KEY "
                "environment variable is set and the anthropic package "
                "is installed."
            ),
        )
    result = data_chat.chat(req.message, req.history)
    return ChatResponse(
        content=result["content"],
        table=result.get("table"),
    )


@app.get("/api/chat/status")
async def chat_status():
    return {"available": data_chat is not None}


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
    records = page.where(pd.notna(page), None).to_dict(orient="records")
    return {
        "name": name,
        "total_rows": total,
        "offset": offset,
        "limit": limit,
        "columns": list(df.columns),
        "rows": records,
    }


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
