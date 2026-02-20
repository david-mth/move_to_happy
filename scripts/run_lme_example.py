"""Quick example of running the LME engine."""

from pathlib import Path

import pandas as pd

from move_to_happy.lme import LMEEngine, UserPreferences

project_root = Path(__file__).resolve().parent.parent
df = pd.read_csv(project_root / "data" / "prepared" / "mth_communities.csv")
engine = LMEEngine(df)
result = engine.score(UserPreferences())

for i, r in enumerate(result.rankings[:10], 1):
    print(f"{i}. {r.city_state} — {r.final_score:.3f}")
