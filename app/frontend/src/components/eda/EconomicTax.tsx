import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import type { EDADataset } from "../../types";

type Row = Record<string, unknown>;

interface Props {
  datasets: EDADataset[];
  stateFilter: string;
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

const STATE_COLORS: Record<string, string> = {
  Georgia: "#6c63ff",
  Alabama: "#22c55e",
  Florida: "#eab308",
};

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function pct25(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length * 0.25)];
}

function pct75(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length * 0.75)];
}

interface StatCard {
  label: string;
  value: string;
}

export function EconomicTax({ datasets, stateFilter, loadData }: Props) {
  const [censusData, setCensusData] = useState<Row[]>([]);
  const [taxData, setTaxData] = useState<Row[]>([]);
  const [commData, setCommData] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      loadData("census", [
        "canonical_id", "median_household_income", "median_home_value",
        "poverty_rate", "unemployment_rate", "state_name",
      ]),
      loadData("tax_rates", [
        "canonical_id", "effective_property_tax_rate",
        "combined_sales_tax_rate", "state_income_tax_rate",
        "state_sales_tax_rate", "avg_local_sales_tax_rate", "state_name",
      ]),
      loadData("communities", [
        "canonical_id", "city_state", "cost_of_living", "population", "state_name",
      ]),
    ])
      .then(([c, t, co]) => {
        setCensusData(c);
        setTaxData(t);
        setCommData(co);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [loadData]);

  const statCards = useMemo((): StatCard[] => {
    const incomes = censusData
      .map((r) => Number(r.median_household_income))
      .filter((v) => !isNaN(v));
    const homes = censusData
      .map((r) => Number(r.median_home_value))
      .filter((v) => !isNaN(v));
    const ptax = taxData
      .map((r) => Number(r.effective_property_tax_rate))
      .filter((v) => !isNaN(v));
    const stax = taxData
      .map((r) => Number(r.combined_sales_tax_rate))
      .filter((v) => !isNaN(v));
    return [
      { label: "Median Income", value: incomes.length ? `$${Math.round(median(incomes)).toLocaleString()}` : "—" },
      { label: "Median Home Value", value: homes.length ? `$${Math.round(median(homes)).toLocaleString()}` : "—" },
      { label: "Avg Property Tax", value: ptax.length ? `${(ptax.reduce((a, b) => a + b, 0) / ptax.length).toFixed(2)}%` : "—" },
      { label: "Avg Sales Tax", value: stax.length ? `${(stax.reduce((a, b) => a + b, 0) / stax.length).toFixed(2)}%` : "—" },
    ];
  }, [censusData, taxData]);

  const boxData = useMemo(() => {
    const states = ["Georgia", "Alabama", "Florida"];
    const metrics = [
      { key: "median_household_income", label: "Income", data: censusData },
      { key: "median_home_value", label: "Home Value", data: censusData },
    ];
    return metrics.map((m) => {
      const row: Record<string, unknown> = { metric: m.label };
      for (const s of states) {
        const vals = m.data
          .filter((r) => r.state_name === s)
          .map((r) => Number(r[m.key]))
          .filter((v) => !isNaN(v));
        row[`${s}_min`] = vals.length ? Math.min(...vals) : 0;
        row[`${s}_q1`] = pct25(vals);
        row[`${s}_med`] = median(vals);
        row[`${s}_q3`] = pct75(vals);
        row[`${s}_max`] = vals.length ? Math.max(...vals) : 0;
      }
      return row;
    });
  }, [censusData]);

  const taxBreakdown = useMemo(() => {
    const states = ["Georgia", "Alabama", "Florida"];
    return states.map((s) => {
      const rows = taxData.filter((r) => r.state_name === s);
      const avg = (key: string) => {
        const vals = rows.map((r) => Number(r[key])).filter((v) => !isNaN(v));
        return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
      };
      return {
        state: s,
        "Property Tax": Number(avg("effective_property_tax_rate").toFixed(3)),
        "State Sales Tax": Number(avg("state_sales_tax_rate").toFixed(3)),
        "Local Sales Tax": Number(avg("avg_local_sales_tax_rate").toFixed(3)),
        "Income Tax": Number(avg("state_income_tax_rate").toFixed(3)),
      };
    });
  }, [taxData]);

  const scatterPoints = useMemo(() => {
    const censusMap = new Map(censusData.map((r) => [r.canonical_id, r]));
    return commData
      .filter((r) => {
        const c = censusMap.get(r.canonical_id);
        return c && c.median_household_income != null && r.cost_of_living != null;
      })
      .map((r) => {
        const c = censusMap.get(r.canonical_id)!;
        return {
          x: Number(r.cost_of_living),
          y: Number(c.median_household_income),
          pop: Number(r.population) || 1000,
          name: String(r.city_state),
          state: String(r.state_name),
        };
      });
  }, [commData, censusData]);

  const scatterGrouped: Record<string, typeof scatterPoints> = {};
  for (const pt of scatterPoints) {
    (scatterGrouped[pt.state] ??= []).push(pt);
  }

  if (loading) {
    return <div className="eda-chart-loading">Loading economic data...</div>;
  }

  return (
    <div className="eda-economic">
      {/* Stat Cards */}
      <div className="eda-stat-cards">
        {statCards.map((c) => (
          <div key={c.label} className="eda-stat-card">
            <div className="eda-stat-value">{c.value}</div>
            <div className="eda-stat-label">{c.label}</div>
          </div>
        ))}
      </div>

      {/* Tax Breakdown */}
      <div className="eda-section">
        <h3 className="eda-section-title">Tax Burden Breakdown by State</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={taxBreakdown} margin={{ top: 10, right: 20, bottom: 10, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis dataKey="state" tick={{ fill: "#8b8fa3", fontSize: 12 }} />
            <YAxis tick={{ fill: "#8b8fa3", fontSize: 11 }} label={{ value: "Rate %", angle: -90, position: "insideLeft", fill: "#8b8fa3", fontSize: 12 }} />
            <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }} />
            <Legend />
            <Bar dataKey="Property Tax" stackId="a" fill="#6c63ff" />
            <Bar dataKey="State Sales Tax" stackId="a" fill="#3b82f6" />
            <Bar dataKey="Local Sales Tax" stackId="a" fill="#22c55e" />
            <Bar dataKey="Income Tax" stackId="a" fill="#eab308" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Box Plot Approximation */}
      <div className="eda-section">
        <h3 className="eda-section-title">Income & Home Value Distribution by State</h3>
        <div className="eda-box-grid">
          {boxData.map((m) => (
            <div key={String(m.metric)} className="eda-box-chart">
              <h4 className="eda-box-title">{String(m.metric)}</h4>
              <div className="eda-box-states">
                {["Georgia", "Alabama", "Florida"].map((s) => {
                  const q1 = Number(m[`${s}_q1`]) || 0;
                  const med = Number(m[`${s}_med`]) || 0;
                  const q3 = Number(m[`${s}_q3`]) || 0;
                  const mn = Number(m[`${s}_min`]) || 0;
                  const mx = Number(m[`${s}_max`]) || 0;
                  return (
                    <div key={s} className="eda-box-row">
                      <span className="eda-box-label" style={{ color: STATE_COLORS[s] }}>{s.slice(0, 2).toUpperCase()}</span>
                      <div className="eda-box-visual">
                        <div className="eda-box-whisker" style={{ left: `${(mn / mx) * 100}%`, width: `${((q1 - mn) / mx) * 100}%` }} />
                        <div className="eda-box-body" style={{ left: `${(q1 / mx) * 100}%`, width: `${((q3 - q1) / mx) * 100}%`, background: STATE_COLORS[s] + "40", borderColor: STATE_COLORS[s] }} />
                        <div className="eda-box-median" style={{ left: `${(med / mx) * 100}%`, background: STATE_COLORS[s] }} />
                      </div>
                      <span className="eda-box-value">${Math.round(med).toLocaleString()}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Income vs COL Scatter */}
      <div className="eda-section">
        <h3 className="eda-section-title">Median Income vs. Cost of Living</h3>
        <p className="eda-section-desc">Larger dots = higher population. Communities in the upper-left are "best value" (high income, low cost).</p>
        <div className="eda-legend">
          {Object.entries(scatterGrouped).map(([state, pts]) => (
            <span key={state} className="eda-legend-item">
              <span className="eda-legend-dot" style={{ background: STATE_COLORS[state] ?? "#8b8fa3" }} />
              {state} ({pts.length})
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={380}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis
              dataKey="x" type="number" name="Cost of Living"
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: "Cost of Living Index", position: "insideBottom", offset: -10, fill: "#8b8fa3", fontSize: 12 }}
            />
            <YAxis
              dataKey="y" type="number" name="Median Income"
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: "Median Income ($)", angle: -90, position: "insideLeft", fill: "#8b8fa3", fontSize: 12 }}
            />
            <ZAxis dataKey="pop" range={[20, 200]} name="Population" />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }}
              labelFormatter={(_, payload) => payload?.[0]?.payload?.name ?? ""}
            />
            {Object.entries(scatterGrouped).map(([state, pts]) => (
              <Scatter key={state} name={state} data={pts} fill={STATE_COLORS[state] ?? "#8b8fa3"} opacity={0.65} />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
