import { useCallback } from "react";
import type { Metadata, ScoreRequest } from "../types";
import { CitySearch } from "./CitySearch";

interface Props {
  prefs: ScoreRequest;
  meta: Metadata | null;
  onChange: (p: ScoreRequest) => void;
  onScore: () => void;
  loading: boolean;
}

const LIFESTYLE_KEYS = [
  ["pref_mountains", "Mountains"],
  ["pref_beach", "Beach"],
  ["pref_lake", "Lake"],
  ["pref_airport", "Airport"],
  ["pref_climate", "Climate"],
  ["pref_terrain", "Terrain"],
  ["pref_cost", "Low Cost"],
] as const;

type LifestyleKey = (typeof LIFESTYLE_KEYS)[number][0];

export function PreferencePanel({ prefs, meta, onChange, onScore, loading }: Props) {
  const set = useCallback(
    <K extends keyof ScoreRequest>(key: K, val: ScoreRequest[K]) => {
      onChange({ ...prefs, [key]: val });
    },
    [prefs, onChange],
  );

  const setLifestyle = useCallback(
    (key: LifestyleKey, raw: number) => {
      const others = LIFESTYLE_KEYS.filter(([k]) => k !== key);
      const othersSum = others.reduce(
        (s, [k]) => s + (prefs[k as keyof ScoreRequest] as number),
        0,
      );
      const clamped = Math.min(raw, 1);
      const remaining = 1 - clamped;
      const patch: Record<string, number> = { [key]: clamped };
      if (othersSum > 0) {
        const scale = remaining / othersSum;
        for (const [k] of others) {
          patch[k] = Math.round(
            (prefs[k as keyof ScoreRequest] as number) * scale * 100,
          ) / 100;
        }
      } else {
        const share = remaining / others.length;
        for (const [k] of others) {
          patch[k] = Math.round(share * 100) / 100;
        }
      }
      onChange({ ...prefs, ...patch });
    },
    [prefs, onChange],
  );

  const handleCitySelect = useCallback(
    (location: { lat: number; lon: number; state: string }) => {
      onChange({ ...prefs, anchor_lat: location.lat, anchor_lon: location.lon, anchor_state: location.state });
    },
    [prefs, onChange],
  );

  const r = meta?.ranges;

  return (
    <div className="pref-panel">
      {/* Financial */}
      <div className="pref-section">
        <h3>Financial</h3>
        <div className="pref-field">
          <label>
            Monthly Payment{" "}
            <span className="value">${prefs.monthly_payment.toLocaleString()}</span>
          </label>
          <input
            type="range"
            min={r?.monthly_payment.min ?? 500}
            max={r?.monthly_payment.max ?? 8000}
            step={r?.monthly_payment.step ?? 100}
            value={prefs.monthly_payment}
            onChange={(e) => set("monthly_payment", +e.target.value)}
          />
        </div>
        <div className="pref-field">
          <label>Loan Term</label>
          <div className="toggle-group">
            {[15, 30].map((y) => (
              <button
                key={y}
                className={prefs.loan_term_years === y ? "active" : ""}
                onClick={() => set("loan_term_years", y)}
              >
                {y} yr
              </button>
            ))}
          </div>
        </div>
        <div className="pref-field">
          <label>
            Down Payment{" "}
            <span className="value">{Math.round(prefs.down_payment_pct * 100)}%</span>
          </label>
          <input
            type="range"
            min={0.03}
            max={0.5}
            step={0.01}
            value={prefs.down_payment_pct}
            onChange={(e) => set("down_payment_pct", +e.target.value)}
          />
        </div>
        <div className="pref-field">
          <label>Bed/Bath Size</label>
          <div className="toggle-group">
            {(meta?.bedbath_buckets ?? ["BB1", "BB2", "BB3"]).map((b) => (
              <button
                key={b}
                className={prefs.bedbath_bucket === b ? "active" : ""}
                onClick={() => set("bedbath_bucket", b)}
              >
                {b}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Location */}
      <div className="pref-section">
        <h3>Location</h3>
        <CitySearch onSelect={handleCitySelect} />
        <div className="pref-field">
          <label>
            Anchor State
          </label>
          <select
            value={prefs.anchor_state}
            onChange={(e) => set("anchor_state", e.target.value)}
          >
            {(meta?.states ?? ["Georgia", "Alabama", "Florida"]).map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </div>
        <div className="pref-field">
          <label>
            Search Radius{" "}
            <span className="value">{prefs.max_radius_miles} mi</span>
          </label>
          <input
            type="range"
            min={r?.max_radius_miles.min ?? 25}
            max={r?.max_radius_miles.max ?? 300}
            step={r?.max_radius_miles.step ?? 5}
            value={prefs.max_radius_miles}
            onChange={(e) => set("max_radius_miles", +e.target.value)}
          />
        </div>
        <div className="pref-field" style={{ display: "flex", gap: "0.5rem" }}>
          <div style={{ flex: 1 }}>
            <label>Latitude</label>
            <input
              type="number"
              step="0.01"
              value={prefs.anchor_lat}
              onChange={(e) => set("anchor_lat", +e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                background: "var(--bg)",
                color: "var(--text)",
                border: "1px solid var(--border)",
                borderRadius: "6px",
                fontSize: "0.8rem",
              }}
            />
          </div>
          <div style={{ flex: 1 }}>
            <label>Longitude</label>
            <input
              type="number"
              step="0.01"
              value={prefs.anchor_lon}
              onChange={(e) => set("anchor_lon", +e.target.value)}
              style={{
                width: "100%",
                padding: "0.4rem 0.6rem",
                background: "var(--bg)",
                color: "var(--text)",
                border: "1px solid var(--border)",
                borderRadius: "6px",
                fontSize: "0.8rem",
              }}
            />
          </div>
        </div>
      </div>

      {/* Lifestyle Weights */}
      <div className="pref-section">
        <h3>Lifestyle Weights</h3>
        {LIFESTYLE_KEYS.map(([key, label]) => (
          <div className="pref-field" key={key}>
            <label>
              {label}{" "}
              <span className="value">
                {Math.round((prefs[key as keyof ScoreRequest] as number) * 100)}%
              </span>
            </label>
            <input
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={prefs[key as keyof ScoreRequest] as number}
              onChange={(e) => setLifestyle(key, +e.target.value)}
            />
          </div>
        ))}
      </div>

      {/* Preferences */}
      <div className="pref-section">
        <h3>Preferences</h3>
        <div className="pref-field">
          <label>Preferred Climate</label>
          <select
            value={prefs.preferred_climate}
            onChange={(e) => set("preferred_climate", e.target.value)}
          >
            {(meta?.climates ?? ["Temperate", "Subtropical"]).map((c) => (
              <option key={c}>{c}</option>
            ))}
          </select>
        </div>
        <div className="pref-field">
          <label>Preferred Terrain</label>
          <select
            value={prefs.preferred_terrain}
            onChange={(e) => set("preferred_terrain", e.target.value)}
          >
            {(meta?.terrains ?? ["Mountains", "Hills", "Plains"]).map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        </div>
      </div>

      <button className="score-btn" onClick={onScore} disabled={loading}>
        {loading ? "Scoring..." : "Find Communities"}
      </button>
    </div>
  );
}
