import { useCallback, useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
  ReferenceLine,
} from "recharts";
import { fetchEDACorrelations, fetchEDAData } from "../../api";
import type { EDACorrelationMatrix } from "../../types";

const NICE_NAMES: Record<string, string> = {
  population: "Population",
  cost_of_living: "Cost of Living",
  median_home_value: "Home Value",
  median_household_income: "Income",
  violent_crime_rate: "Violent Crime",
  property_crime_rate: "Property Crime",
  pct_broadband_100_20: "Broadband %",
  pm25_mean: "PM2.5",
  avg_annual_salary: "Avg Salary",
  nearest_hospital_miles: "Hospital Dist",
  providers_per_1000_pop: "Providers/1k",
  hs_graduation_rate: "HS Grad Rate",
  effective_property_tax_rate: "Property Tax",
  combined_sales_tax_rate: "Sales Tax",
  lake_distance_miles: "Lake Distance",
  lake_area_sq_mi: "Lake Area",
};

function corrColor(val: number): string {
  if (val >= 0.6) return "#22c55e";
  if (val >= 0.3) return "#3b82f6";
  if (val >= 0) return "#1a1d27";
  if (val >= -0.3) return "#1a1d27";
  if (val >= -0.6) return "#f97316";
  return "#ef4444";
}

function corrOpacity(val: number): number {
  return Math.min(1, Math.abs(val) * 1.2 + 0.1);
}

function linearRegression(points: { x: number; y: number }[]) {
  const n = points.length;
  if (n < 2) return null;
  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (const p of points) {
    sx += p.x;
    sy += p.y;
    sxx += p.x * p.x;
    sxy += p.x * p.y;
  }
  const denom = n * sxx - sx * sx;
  if (Math.abs(denom) < 1e-10) return null;
  const slope = (n * sxy - sx * sy) / denom;
  const intercept = (sy - slope * sx) / n;
  return { slope, intercept };
}

function pearsonR(points: { x: number; y: number }[]) {
  const n = points.length;
  if (n < 2) return 0;
  let sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
  for (const p of points) {
    sx += p.x;
    sy += p.y;
    sxx += p.x * p.x;
    syy += p.y * p.y;
    sxy += p.x * p.y;
  }
  const num = n * sxy - sx * sy;
  const den = Math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy));
  return den === 0 ? 0 : num / den;
}

export function CorrelationExplorer() {
  const [corrData, setCorrData] = useState<EDACorrelationMatrix | null>(null);
  const [xCol, setXCol] = useState("");
  const [yCol, setYCol] = useState("");
  const [sizeCol, setSizeCol] = useState("");
  const [showTrend, setShowTrend] = useState(true);
  const [scatterPoints, setScatterPoints] = useState<
    { x: number; y: number; size: number; name: string; state: string }[]
  >([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchEDACorrelations()
      .then((data) => {
        setCorrData(data);
        if (data.columns.length >= 2) {
          setXCol(data.columns[0]);
          setYCol(data.columns[1]);
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const loadScatter = useCallback(async () => {
    if (!xCol || !yCol) return;
    const cols = [xCol, yCol, "state_name", "city_state", "canonical_id"];
    if (sizeCol) cols.push(sizeCol);
    const result = await fetchEDAData("communities", cols);
    const pts = result.rows
      .filter((r) => r[xCol] != null && r[yCol] != null && !isNaN(Number(r[xCol])) && !isNaN(Number(r[yCol])))
      .map((r) => ({
        x: Number(r[xCol]),
        y: Number(r[yCol]),
        size: sizeCol && r[sizeCol] != null ? Number(r[sizeCol]) : 100,
        name: String(r.city_state ?? r.canonical_id ?? ""),
        state: String(r.state_name ?? ""),
      }));
    setScatterPoints(pts);
  }, [xCol, yCol, sizeCol]);

  useEffect(() => {
    loadScatter();
  }, [loadScatter]);

  const r = useMemo(() => pearsonR(scatterPoints), [scatterPoints]);
  const regression = useMemo(
    () => (showTrend ? linearRegression(scatterPoints) : null),
    [scatterPoints, showTrend],
  );

  const STATE_COLORS: Record<string, string> = {
    Georgia: "#6c63ff",
    Alabama: "#22c55e",
    Florida: "#eab308",
  };

  const scatterGrouped: Record<string, typeof scatterPoints> = {};
  for (const pt of scatterPoints) {
    (scatterGrouped[pt.state] ??= []).push(pt);
  }

  const trendLinePoints = useMemo(() => {
    if (!regression || !scatterPoints.length) return [];
    const xs = scatterPoints.map((p) => p.x);
    const xMin = Math.min(...xs);
    const xMax = Math.max(...xs);
    return [
      { x: xMin, y: regression.slope * xMin + regression.intercept },
      { x: xMax, y: regression.slope * xMax + regression.intercept },
    ];
  }, [regression, scatterPoints]);

  if (loading) {
    return <div className="eda-chart-loading">Computing correlations...</div>;
  }

  const columns = corrData?.columns ?? [];

  return (
    <div className="eda-correlation">
      {/* Correlation Matrix */}
      <div className="eda-section">
        <h3 className="eda-section-title">Pairwise Correlation Matrix</h3>
        <p className="eda-section-desc">Click a cell to explore that pair in the scatter plot below.</p>
        <div className="eda-corr-matrix-wrap">
          <table className="eda-corr-matrix">
            <thead>
              <tr>
                <th></th>
                {columns.map((c) => (
                  <th key={c} title={c}>
                    <span className="eda-corr-header">{NICE_NAMES[c] ?? c}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {corrData?.matrix.map((row) => {
                const rowName = String(row.column);
                return (
                  <tr key={rowName}>
                    <td className="eda-corr-row-label" title={rowName}>
                      {NICE_NAMES[rowName] ?? rowName}
                    </td>
                    {columns.map((c) => {
                      const val = Number(row[c] ?? 0);
                      const isSelected = (xCol === rowName && yCol === c) || (xCol === c && yCol === rowName);
                      return (
                        <td
                          key={c}
                          className={`eda-corr-cell ${isSelected ? "selected" : ""}`}
                          style={{
                            background: corrColor(val),
                            opacity: corrOpacity(val),
                          }}
                          title={`${NICE_NAMES[rowName] ?? rowName} vs ${NICE_NAMES[c] ?? c}: r=${val.toFixed(3)}`}
                          onClick={() => {
                            if (rowName !== c) {
                              setXCol(rowName);
                              setYCol(c);
                            }
                          }}
                        >
                          {rowName === c ? "" : val.toFixed(2)}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Enhanced Scatter Plot */}
      <div className="eda-section">
        <div className="eda-section-header">
          <h3 className="eda-section-title">
            {NICE_NAMES[xCol] ?? xCol} vs. {NICE_NAMES[yCol] ?? yCol}
          </h3>
          <div className="eda-corr-stats">
            <span className="eda-corr-r">r = {r.toFixed(3)}</span>
            <span className="eda-corr-r2">r² = {(r * r).toFixed(3)}</span>
          </div>
        </div>
        <div className="eda-controls">
          <label>
            X Axis
            <select value={xCol} onChange={(e) => setXCol(e.target.value)}>
              {columns.map((c) => (
                <option key={c} value={c}>{NICE_NAMES[c] ?? c}</option>
              ))}
            </select>
          </label>
          <label>
            Y Axis
            <select value={yCol} onChange={(e) => setYCol(e.target.value)}>
              {columns.map((c) => (
                <option key={c} value={c}>{NICE_NAMES[c] ?? c}</option>
              ))}
            </select>
          </label>
          <label>
            Dot Size
            <select value={sizeCol} onChange={(e) => setSizeCol(e.target.value)}>
              <option value="">Uniform</option>
              {columns.map((c) => (
                <option key={c} value={c}>{NICE_NAMES[c] ?? c}</option>
              ))}
            </select>
          </label>
          <label>
            Trend Line
            <div className="eda-toggle">
              <button className={showTrend ? "active" : ""} onClick={() => setShowTrend(true)}>On</button>
              <button className={!showTrend ? "active" : ""} onClick={() => setShowTrend(false)}>Off</button>
            </div>
          </label>
        </div>
        <div className="eda-legend">
          {Object.entries(scatterGrouped).map(([state, pts]) => (
            <span key={state} className="eda-legend-item">
              <span className="eda-legend-dot" style={{ background: STATE_COLORS[state] ?? "#8b8fa3" }} />
              {state} ({pts.length})
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={420}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis
              dataKey="x" type="number" name={xCol}
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: NICE_NAMES[xCol] ?? xCol, position: "insideBottom", offset: -10, fill: "#8b8fa3", fontSize: 12 }}
            />
            <YAxis
              dataKey="y" type="number" name={yCol}
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: NICE_NAMES[yCol] ?? yCol, angle: -90, position: "insideLeft", fill: "#8b8fa3", fontSize: 12 }}
            />
            <ZAxis dataKey="size" range={sizeCol ? [15, 150] : [25, 25]} />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }}
              labelFormatter={(_, payload) => payload?.[0]?.payload?.name ?? ""}
            />
            {showTrend && trendLinePoints.length === 2 && (
              <ReferenceLine
                segment={[
                  { x: trendLinePoints[0].x, y: trendLinePoints[0].y },
                  { x: trendLinePoints[1].x, y: trendLinePoints[1].y },
                ]}
                stroke="#ef4444"
                strokeWidth={2}
                strokeDasharray="6 4"
              />
            )}
            {Object.entries(scatterGrouped).map(([state, pts]) => (
              <Scatter key={state} name={state} data={pts} fill={STATE_COLORS[state] ?? "#8b8fa3"} opacity={0.6} />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
