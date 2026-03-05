import { useCallback, useEffect, useState } from "react";
import {
  CartesianGrid,
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

export function ScatterPlot({ datasets, stateFilter, loadData }: Props) {
  const [selectedDs, setSelectedDs] = useState("");
  const [xCol, setXCol] = useState("");
  const [yCol, setYCol] = useState("");
  const [plotData, setPlotData] = useState<
    { x: number; y: number; name: string; state: string }[]
  >([]);
  const [loading, setLoading] = useState(false);

  const numericCols =
    datasets.find((d) => d.name === selectedDs)?.numeric_columns ?? [];

  useEffect(() => {
    if (datasets.length && !selectedDs) {
      const ds = datasets.find((d) => d.name === "communities") ?? datasets[0];
      setSelectedDs(ds.name);
    }
  }, [datasets, selectedDs]);

  useEffect(() => {
    if (numericCols.length >= 2) {
      if (!numericCols.find((c) => c.name === xCol)) setXCol(numericCols[0].name);
      if (!numericCols.find((c) => c.name === yCol))
        setYCol(numericCols[Math.min(1, numericCols.length - 1)].name);
    }
  }, [numericCols, xCol, yCol]);

  const refresh = useCallback(async () => {
    if (!selectedDs || !xCol || !yCol) return;
    setLoading(true);
    try {
      const cols = [xCol, yCol, "state_name", "canonical_id", "city_state"];
      const rows = await loadData(selectedDs, cols);
      let filtered = rows;
      if (stateFilter !== "All") {
        filtered = rows.filter((r) => r.state_name === stateFilter);
      }
      const pts = filtered
        .filter(
          (r) =>
            r[xCol] != null &&
            r[yCol] != null &&
            !isNaN(Number(r[xCol])) &&
            !isNaN(Number(r[yCol])),
        )
        .map((r) => ({
          x: Number(r[xCol]),
          y: Number(r[yCol]),
          name: String(r.city_state ?? r.canonical_id ?? ""),
          state: String(r.state_name ?? ""),
        }));
      setPlotData(pts);
    } catch {
      setPlotData([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDs, xCol, yCol, stateFilter, loadData]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const grouped: Record<string, typeof plotData> = {};
  for (const pt of plotData) {
    const key = pt.state || "Unknown";
    (grouped[key] ??= []).push(pt);
  }

  return (
    <div className="eda-chart-container">
      <div className="eda-controls">
        <label>
          Dataset
          <select
            value={selectedDs}
            onChange={(e) => setSelectedDs(e.target.value)}
          >
            {datasets.map((d) => (
              <option key={d.name} value={d.name}>
                {d.name} ({d.rows} rows)
              </option>
            ))}
          </select>
        </label>
        <label>
          X Axis
          <select value={xCol} onChange={(e) => setXCol(e.target.value)}>
            {numericCols.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Y Axis
          <select value={yCol} onChange={(e) => setYCol(e.target.value)}>
            {numericCols.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
      </div>
      {loading ? (
        <div className="eda-chart-loading">Loading...</div>
      ) : (
        <>
          <div className="eda-legend">
            {Object.keys(grouped).map((state) => (
              <span key={state} className="eda-legend-item">
                <span
                  className="eda-legend-dot"
                  style={{ background: STATE_COLORS[state] ?? "#8b8fa3" }}
                />
                {state} ({grouped[state].length})
              </span>
            ))}
          </div>
          <ResponsiveContainer width="100%" height={380}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
              <XAxis
                dataKey="x"
                type="number"
                name={xCol}
                tick={{ fill: "#8b8fa3", fontSize: 11 }}
                label={{
                  value: xCol,
                  position: "insideBottom",
                  offset: -10,
                  fill: "#8b8fa3",
                  fontSize: 12,
                }}
              />
              <YAxis
                dataKey="y"
                type="number"
                name={yCol}
                tick={{ fill: "#8b8fa3", fontSize: 11 }}
                label={{
                  value: yCol,
                  angle: -90,
                  position: "insideLeft",
                  fill: "#8b8fa3",
                  fontSize: 12,
                }}
              />
              <ZAxis range={[20, 20]} />
              <Tooltip
                cursor={{ strokeDasharray: "3 3" }}
                contentStyle={{
                  background: "#1a1d27",
                  border: "1px solid #2a2e3e",
                  borderRadius: 8,
                  fontSize: 12,
                }}
                formatter={(value: number | undefined, name: string) => [
                  (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 }),
                  name === "x" ? xCol : yCol,
                ]}
                labelFormatter={(_, payload) =>
                  payload?.[0]?.payload?.name ?? ""
                }
              />
              {Object.entries(grouped).map(([state, pts]) => (
                <Scatter
                  key={state}
                  name={state}
                  data={pts}
                  fill={STATE_COLORS[state] ?? "#8b8fa3"}
                  opacity={0.7}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
