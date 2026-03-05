import { useCallback, useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { EDADataset } from "../../types";

type Row = Record<string, unknown>;

interface Props {
  datasets: EDADataset[];
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

type AggMode = "mean" | "median";

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

const STATE_COLORS: Record<string, string> = {
  Georgia: "#6c63ff",
  Alabama: "#22c55e",
  Florida: "#eab308",
};

export function StateComparison({ datasets, loadData }: Props) {
  const [selectedDs, setSelectedDs] = useState("");
  const [selectedCol, setSelectedCol] = useState("");
  const [aggMode, setAggMode] = useState<AggMode>("mean");
  const [chartData, setChartData] = useState<Record<string, unknown>[]>([]);
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
      const rows = await loadData(selectedDs, [selectedCol, "state_name"]);
      const byState: Record<string, number[]> = {};
      for (const r of rows) {
        const state = String(r.state_name ?? "Unknown");
        const val = Number(r[selectedCol]);
        if (!isNaN(val)) (byState[state] ??= []).push(val);
      }
      const result = Object.entries(byState).map(([state, vals]) => ({
        state,
        value:
          aggMode === "mean"
            ? vals.reduce((a, b) => a + b, 0) / vals.length
            : median(vals),
        count: vals.length,
      }));
      result.sort((a, b) => b.value - a.value);
      setChartData(result);
    } catch {
      setChartData([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDs, selectedCol, aggMode, loadData]);

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
          Aggregation
          <div className="eda-toggle">
            <button
              className={aggMode === "mean" ? "active" : ""}
              onClick={() => setAggMode("mean")}
            >
              Mean
            </button>
            <button
              className={aggMode === "median" ? "active" : ""}
              onClick={() => setAggMode("median")}
            >
              Median
            </button>
          </div>
        </label>
      </div>
      {loading ? (
        <div className="eda-chart-loading">Loading...</div>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <BarChart
            data={chartData}
            margin={{ top: 10, right: 20, bottom: 10, left: 20 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis
              dataKey="state"
              tick={{ fill: "#8b8fa3", fontSize: 12 }}
            />
            <YAxis tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <Tooltip
              contentStyle={{
                background: "#1a1d27",
                border: "1px solid #2a2e3e",
                borderRadius: 8,
                fontSize: 12,
              }}
              formatter={(value: number | undefined) => [
                (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 }),
                `${aggMode} ${selectedCol}`,
              ]}
            />
            <Legend />
            <Bar
              dataKey="value"
              name={`${aggMode} ${selectedCol}`}
              radius={[4, 4, 0, 0]}
            >
              {chartData.map((entry, idx) => (
                <Cell
                  key={idx}
                  fill={STATE_COLORS[String(entry.state)] ?? "#8b8fa3"}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
