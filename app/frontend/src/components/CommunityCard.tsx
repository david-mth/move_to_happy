import type { CommunityScore } from "../types";
import { ScoreBars } from "./ScoreBars";

interface Props {
  community: CommunityScore;
  rank: number;
  isSelected: boolean;
  onSelect: () => void;
}

function fmt(val: number | undefined, prefix = "", suffix = "", decimals = 0): string {
  if (val === undefined || val === null) return "—";
  return `${prefix}${Number(val).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })}${suffix}`;
}

export function CommunityCard({ community: c, rank, isSelected, onSelect }: Props) {
  const e = c.enrichment;

  return (
    <div
      className={`community-card${isSelected ? " selected" : ""}`}
      onClick={onSelect}
    >
      <div className="card-header">
        <div>
          <div className="card-rank">#{rank}</div>
          <div className="card-name">{c.city_state}</div>
          <div className="card-meta">
            {c.terrain} · {c.climate} · Pop {c.population.toLocaleString()} ·{" "}
            {c.dist_to_anchor.toFixed(0)} mi away
          </div>
        </div>
        <div className="card-score-badge">{c.final_score.toFixed(3)}</div>
      </div>

      <ScoreBars
        housing={c.housing_score}
        lifestyle={c.lifestyle_score}
        spillover={c.spillover_score}
      />

      <div className="card-meta" style={{ marginBottom: "0.4rem" }}>
        Median home: <strong>${c.median_home_price.toLocaleString()}</strong> · COL:{" "}
        <strong>{c.cost_of_living.toFixed(0)}</strong> · Pressure:{" "}
        <strong>{c.pressure}</strong>
      </div>

      {isSelected && (
        <>
          <div className="enrichment-grid">
            <EnrichItem label="Median Income" value={fmt(e.median_household_income, "$")} />
            <EnrichItem label="Crime Rate" value={fmt(e.violent_crime_rate, "", "/100k")} />
            <EnrichItem
              label="Broadband 100/20"
              value={fmt(e.pct_broadband_100_20, "", "%", 1)}
            />
            <EnrichItem label="PM2.5" value={fmt(e.pm25_mean, "", " µg/m³", 1)} />
            <EnrichItem label="Avg Wage" value={fmt(e.avg_weekly_wage, "$", "/wk")} />
            <EnrichItem
              label="Hospital"
              value={
                e.nearest_hospital_miles !== undefined
                  ? `${e.nearest_hospital_miles.toFixed(1)} mi`
                  : "—"
              }
            />
            <EnrichItem
              label="Hosp Rating"
              value={fmt(e.nearest_hospital_rating, "", "★", 1)}
            />
            <EnrichItem
              label="Providers/1k"
              value={fmt(e.providers_per_1000_pop, "", "", 1)}
            />
            <EnrichItem label="Median Rent" value={fmt(e.median_rent, "$")} />
          </div>

          {c.spillover_explanation && (
            <div className="spillover-note">{c.spillover_explanation}</div>
          )}
        </>
      )}
    </div>
  );
}

function EnrichItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="enrich-item">
      <div className="enrich-value">{value}</div>
      <div className="enrich-label">{label}</div>
    </div>
  );
}
