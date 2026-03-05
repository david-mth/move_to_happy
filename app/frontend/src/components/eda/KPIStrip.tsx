import type { EDAKPIs } from "../../types";

interface Props {
  kpis: EDAKPIs;
}

function fmt(val: number | null, opts?: { prefix?: string; suffix?: string; decimals?: number }) {
  if (val == null) return "—";
  const d = opts?.decimals ?? 0;
  const num = d === 0
    ? Math.round(val).toLocaleString()
    : val.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
  return `${opts?.prefix ?? ""}${num}${opts?.suffix ?? ""}`;
}

const CARDS: {
  key: keyof EDAKPIs;
  label: string;
  prefix?: string;
  suffix?: string;
  decimals?: number;
}[] = [
  { key: "total_communities", label: "Communities" },
  { key: "median_home_value", label: "Median Home Value", prefix: "$" },
  { key: "median_income", label: "Median Income", prefix: "$" },
  { key: "avg_violent_crime_rate", label: "Avg Crime Rate", suffix: " /100k", decimals: 1 },
  { key: "avg_broadband_pct", label: "Avg Broadband", suffix: "%", decimals: 1 },
  { key: "avg_property_tax_rate", label: "Avg Property Tax", suffix: "%", decimals: 2 },
  { key: "avg_lake_distance", label: "Avg Lake Dist", suffix: " mi", decimals: 1 },
];

export function KPIStrip({ kpis }: Props) {
  return (
    <div className="eda-kpi-strip">
      {CARDS.map((c) => (
        <div key={c.key} className="eda-kpi-card">
          <div className="eda-kpi-value">
            {fmt(kpis[c.key], { prefix: c.prefix, suffix: c.suffix, decimals: c.decimals })}
          </div>
          <div className="eda-kpi-label">{c.label}</div>
        </div>
      ))}
    </div>
  );
}
