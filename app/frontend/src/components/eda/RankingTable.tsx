import { useCallback, useEffect, useState } from "react";
import type { EDADataset } from "../../types";

type Row = Record<string, unknown>;

interface Props {
  datasets: EDADataset[];
  stateFilter: string;
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

type Direction = "top" | "bottom";

export function RankingTable({ datasets, stateFilter, loadData }: Props) {
  const [selectedDs, setSelectedDs] = useState("");
  const [selectedCol, setSelectedCol] = useState("");
  const [direction, setDirection] = useState<Direction>("top");
  const [count, setCount] = useState(20);
  const [rows, setRows] = useState<
    { rank: number; name: string; state: string; value: number }[]
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
    if (numericCols.length && !numericCols.find((c) => c.name === selectedCol)) {
      setSelectedCol(numericCols[0].name);
    }
  }, [numericCols, selectedCol]);

  const refresh = useCallback(async () => {
    if (!selectedDs || !selectedCol) return;
    setLoading(true);
    try {
      const cols = [
        selectedCol,
        "state_name",
        "city_state",
        "canonical_id",
      ];
      const rawRows = await loadData(selectedDs, cols);
      let filtered = rawRows;
      if (stateFilter !== "All") {
        filtered = rawRows.filter((r) => r.state_name === stateFilter);
      }
      const valid = filtered
        .filter((r) => r[selectedCol] != null && !isNaN(Number(r[selectedCol])))
        .map((r) => ({
          name: String(r.city_state ?? r.canonical_id ?? ""),
          state: String(r.state_name ?? ""),
          value: Number(r[selectedCol]),
        }));
      valid.sort((a, b) =>
        direction === "top" ? b.value - a.value : a.value - b.value,
      );
      setRows(
        valid.slice(0, count).map((r, i) => ({ rank: i + 1, ...r })),
      );
    } catch {
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDs, selectedCol, direction, count, stateFilter, loadData]);

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
          Rank by
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
          Direction
          <div className="eda-toggle">
            <button
              className={direction === "top" ? "active" : ""}
              onClick={() => setDirection("top")}
            >
              Top
            </button>
            <button
              className={direction === "bottom" ? "active" : ""}
              onClick={() => setDirection("bottom")}
            >
              Bottom
            </button>
          </div>
        </label>
        <label>
          Show
          <select
            value={count}
            onChange={(e) => setCount(Number(e.target.value))}
          >
            {[10, 20, 50].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
      </div>
      {loading ? (
        <div className="eda-chart-loading">Loading...</div>
      ) : (
        <div className="eda-ranking-wrap">
          <table className="eda-ranking-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Community</th>
                <th>State</th>
                <th>{selectedCol}</th>
                <th>Bar</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const maxVal = rows[0]?.value || 1;
                const pct = Math.abs(r.value / maxVal) * 100;
                return (
                  <tr key={r.rank}>
                    <td className="eda-rank-num">{r.rank}</td>
                    <td className="eda-rank-name">{r.name}</td>
                    <td className="eda-rank-state">{r.state}</td>
                    <td className="eda-rank-value">
                      {r.value.toLocaleString(undefined, {
                        maximumFractionDigits: 2,
                      })}
                    </td>
                    <td className="eda-rank-bar-cell">
                      <div className="eda-rank-bar-track">
                        <div
                          className="eda-rank-bar-fill"
                          style={{ width: `${Math.min(pct, 100)}%` }}
                        />
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
