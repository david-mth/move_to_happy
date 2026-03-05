import { useCallback, useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { EDADataset } from "../../types";

type Row = Record<string, unknown>;

interface Props {
  datasets: EDADataset[];
  stateFilter: string;
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

function buildHistogram(values: number[], bins: number) {
  if (!values.length) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) return [{ range: String(min), count: values.length }];
  const step = (max - min) / bins;
  const buckets = Array.from({ length: bins }, (_, i) => ({
    lo: min + i * step,
    hi: min + (i + 1) * step,
    count: 0,
  }));
  for (const v of values) {
    const idx = Math.min(Math.floor((v - min) / step), bins - 1);
    buckets[idx].count++;
  }
  return buckets.map((b) => ({
    range: `${b.lo.toFixed(1)}-${b.hi.toFixed(1)}`,
    count: b.count,
  }));
}

export function DistributionChart({ datasets, stateFilter, loadData }: Props) {
  const [selectedDs, setSelectedDs] = useState("");
  const [selectedCol, setSelectedCol] = useState("");
  const [bins, setBins] = useState(20);
  const [data, setData] = useState<{ range: string; count: number }[]>([]);
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
    if (numericCols.length && !numericCols.find((c) => c.name === selectedCol)) {
      setSelectedCol(numericCols[0].name);
    }
  }, [numericCols, selectedCol]);

  const refresh = useCallback(async () => {
    if (!selectedDs || !selectedCol) return;
    setLoading(true);
    try {
      const cols = [selectedCol];
      if (stateFilter !== "All") cols.push("state_name");
      const rows = await loadData(selectedDs, cols);
      let filtered = rows;
      if (stateFilter !== "All") {
        filtered = rows.filter((r) => r.state_name === stateFilter);
      }
      const values = filtered
        .map((r) => Number(r[selectedCol]))
        .filter((v) => !isNaN(v));
      setData(buildHistogram(values, bins));
    } catch {
      setData([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDs, selectedCol, bins, stateFilter, loadData]);

  useEffect(() => {
    refresh();
  }, [refresh]);

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
          Column
          <select
            value={selectedCol}
            onChange={(e) => setSelectedCol(e.target.value)}
          >
            {numericCols.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Bins
          <input
            type="range"
            min={5}
            max={50}
            value={bins}
            onChange={(e) => setBins(Number(e.target.value))}
          />
          <span className="eda-value">{bins}</span>
        </label>
      </div>
      {loading ? (
        <div className="eda-chart-loading">Loading...</div>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <BarChart data={data} margin={{ top: 10, right: 20, bottom: 40, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis
              dataKey="range"
              tick={{ fill: "#8b8fa3", fontSize: 10 }}
              angle={-35}
              textAnchor="end"
              interval={Math.max(0, Math.floor(data.length / 12))}
            />
            <YAxis tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <Tooltip
              contentStyle={{
                background: "#1a1d27",
                border: "1px solid #2a2e3e",
                borderRadius: 8,
                fontSize: 12,
              }}
            />
            <Bar dataKey="count" fill="#6c63ff" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
