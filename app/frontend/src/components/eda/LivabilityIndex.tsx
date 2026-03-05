import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import type { CommunityIndex } from "../../types";

interface Props {
  indices: CommunityIndex[];
  stateFilter: string;
}

const INDEX_KEYS = [
  "affordability", "safety", "healthcare", "education", "digital", "environmental",
] as const;

const INDEX_LABELS: Record<string, string> = {
  affordability: "Affordability",
  safety: "Safety",
  healthcare: "Healthcare",
  education: "Education",
  digital: "Digital",
  environmental: "Environment",
};

const STATE_COLORS: Record<string, string> = {
  Georgia: "#6c63ff",
  Alabama: "#22c55e",
  Florida: "#eab308",
};

type AggMode = "mean" | "median";

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export function LivabilityIndex({ indices, stateFilter }: Props) {
  const [selectedId, setSelectedId] = useState("");
  const [search, setSearch] = useState("");
  const [aggMode, setAggMode] = useState<AggMode>("mean");
  const [xIdx, setXIdx] = useState<string>("affordability");
  const [yIdx, setYIdx] = useState<string>("safety");

  const filtered = useMemo(() => {
    if (stateFilter === "All") return indices;
    return indices.filter((c) => c.state_name === stateFilter);
  }, [indices, stateFilter]);

  const searchResults = useMemo(() => {
    if (!search) return [];
    const q = search.toLowerCase();
    return filtered.filter((c) => c.city_state.toLowerCase().includes(q)).slice(0, 8);
  }, [filtered, search]);

  const selected = useMemo(
    () => filtered.find((c) => c.canonical_id === selectedId) ?? null,
    [filtered, selectedId],
  );

  const stateAvg = useMemo(() => {
    const state = selected?.state_name ?? stateFilter;
    const pool = state === "All" ? indices : indices.filter((c) => c.state_name === state);
    return INDEX_KEYS.reduce(
      (acc, k) => {
        const vals = pool.map((c) => c[k]).filter((v): v is number => v != null);
        acc[k] = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
        return acc;
      },
      {} as Record<string, number>,
    );
  }, [indices, selected, stateFilter]);

  const radarData = useMemo(() => {
    return INDEX_KEYS.map((k) => ({
      dimension: INDEX_LABELS[k],
      community: selected?.[k] ?? 0,
      stateAvg: Math.round(stateAvg[k]),
    }));
  }, [selected, stateAvg]);

  const stateCompData = useMemo(() => {
    const states = ["Georgia", "Alabama", "Florida"];
    return INDEX_KEYS.map((k) => {
      const row: Record<string, unknown> = { dimension: INDEX_LABELS[k] };
      for (const s of states) {
        const vals = indices
          .filter((c) => c.state_name === s)
          .map((c) => c[k])
          .filter((v): v is number => v != null);
        row[s] = vals.length
          ? Math.round(aggMode === "mean" ? vals.reduce((a, b) => a + b, 0) / vals.length : median(vals))
          : 0;
      }
      return row;
    });
  }, [indices, aggMode]);

  const scatterData = useMemo(() => {
    return filtered
      .filter((c) => c[xIdx as keyof CommunityIndex] != null && c[yIdx as keyof CommunityIndex] != null)
      .map((c) => ({
        x: c[xIdx as keyof CommunityIndex] as number,
        y: c[yIdx as keyof CommunityIndex] as number,
        name: c.city_state,
        state: c.state_name,
      }));
  }, [filtered, xIdx, yIdx]);

  const scatterGrouped: Record<string, typeof scatterData> = {};
  for (const pt of scatterData) {
    (scatterGrouped[pt.state] ??= []).push(pt);
  }

  return (
    <div className="eda-livability">
      {/* Radar Chart */}
      <div className="eda-section">
        <h3 className="eda-section-title">Community Profile vs. State Average</h3>
        <div className="eda-radar-controls">
          <input
            className="eda-rank-search"
            placeholder="Search for a community..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              if (!e.target.value) setSelectedId("");
            }}
          />
          {searchResults.length > 0 && (
            <div className="eda-search-dropdown">
              {searchResults.map((c) => (
                <button
                  key={c.canonical_id}
                  className="eda-search-item"
                  onClick={() => {
                    setSelectedId(c.canonical_id);
                    setSearch(c.city_state);
                  }}
                >
                  <span className="eda-search-name">{c.city_state}</span>
                  <span className="eda-search-state">{c.state_name}</span>
                </button>
              ))}
            </div>
          )}
        </div>
        {selected ? (
          <ResponsiveContainer width="100%" height={340}>
            <RadarChart data={radarData} outerRadius="75%">
              <PolarGrid stroke="#2a2e3e" />
              <PolarAngleAxis dataKey="dimension" tick={{ fill: "#8b8fa3", fontSize: 12 }} />
              <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fill: "#8b8fa3", fontSize: 10 }} />
              <Radar name={selected.city_state} dataKey="community" stroke="#6c63ff" fill="#6c63ff" fillOpacity={0.3} />
              <Radar name="State Avg" dataKey="stateAvg" stroke="#8b8fa3" fill="#8b8fa3" fillOpacity={0.1} strokeDasharray="4 4" />
              <Legend />
              <Tooltip
                contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }}
              />
            </RadarChart>
          </ResponsiveContainer>
        ) : (
          <div className="eda-chart-placeholder">Search and select a community to see its radar profile</div>
        )}
      </div>

      {/* State Comparison */}
      <div className="eda-section">
        <div className="eda-section-header">
          <h3 className="eda-section-title">Index Comparison by State</h3>
          <div className="eda-toggle">
            <button className={aggMode === "mean" ? "active" : ""} onClick={() => setAggMode("mean")}>Mean</button>
            <button className={aggMode === "median" ? "active" : ""} onClick={() => setAggMode("median")}>Median</button>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={stateCompData} margin={{ top: 10, right: 20, bottom: 10, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis dataKey="dimension" tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <YAxis domain={[0, 100]} tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }} />
            <Legend />
            <Bar dataKey="Georgia" fill="#6c63ff" radius={[3, 3, 0, 0]} />
            <Bar dataKey="Alabama" fill="#22c55e" radius={[3, 3, 0, 0]} />
            <Bar dataKey="Florida" fill="#eab308" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Index Scatter */}
      <div className="eda-section">
        <h3 className="eda-section-title">Index vs. Index</h3>
        <div className="eda-controls">
          <label>
            X Axis
            <select value={xIdx} onChange={(e) => setXIdx(e.target.value)}>
              {INDEX_KEYS.map((k) => (
                <option key={k} value={k}>{INDEX_LABELS[k]}</option>
              ))}
            </select>
          </label>
          <label>
            Y Axis
            <select value={yIdx} onChange={(e) => setYIdx(e.target.value)}>
              {INDEX_KEYS.map((k) => (
                <option key={k} value={k}>{INDEX_LABELS[k]}</option>
              ))}
            </select>
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
        <ResponsiveContainer width="100%" height={360}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis
              dataKey="x" type="number" name={xIdx} domain={[0, 100]}
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: INDEX_LABELS[xIdx], position: "insideBottom", offset: -10, fill: "#8b8fa3", fontSize: 12 }}
            />
            <YAxis
              dataKey="y" type="number" name={yIdx} domain={[0, 100]}
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: INDEX_LABELS[yIdx], angle: -90, position: "insideLeft", fill: "#8b8fa3", fontSize: 12 }}
            />
            <ZAxis range={[25, 25]} />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }}
              formatter={(value: number | undefined) => [(value ?? 0).toFixed(1)]}
              labelFormatter={(_, payload) => payload?.[0]?.payload?.name ?? ""}
            />
            {Object.entries(scatterGrouped).map(([state, pts]) => (
              <Scatter key={state} name={state} data={pts} fill={STATE_COLORS[state] ?? "#8b8fa3"} opacity={0.7} />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
