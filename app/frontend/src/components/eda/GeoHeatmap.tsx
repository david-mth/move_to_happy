import { useCallback, useEffect, useMemo, useState } from "react";
import type { EDADataset } from "../../types";

type Row = Record<string, unknown>;

interface Props {
  datasets: EDADataset[];
  stateFilter: string;
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

function valueToColor(val: number, min: number, max: number): string {
  if (max === min) return "rgba(108, 99, 255, 0.5)";
  const t = (val - min) / (max - min);
  const r = Math.round(34 + t * (108 - 34));
  const g = Math.round(197 + t * (99 - 197));
  const b = Math.round(94 + t * (255 - 94));
  return `rgba(${r}, ${g}, ${b}, 0.85)`;
}

export function GeoHeatmap({ datasets, stateFilter, loadData }: Props) {
  const [selectedDs, setSelectedDs] = useState("");
  const [selectedCol, setSelectedCol] = useState("");
  const [points, setPoints] = useState<
    { lat: number; lon: number; val: number; name: string }[]
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
      const preferred = numericCols.find((c) => c.name === "population");
      setSelectedCol(preferred?.name ?? numericCols[0].name);
    }
  }, [numericCols, selectedCol]);

  const refresh = useCallback(async () => {
    if (!selectedDs || !selectedCol) return;
    setLoading(true);
    try {
      const cols = [
        selectedCol,
        "latitude",
        "longitude",
        "state_name",
        "city_state",
        "canonical_id",
      ];
      const rows = await loadData(selectedDs, cols);
      let filtered = rows;
      if (stateFilter !== "All") {
        filtered = rows.filter((r) => r.state_name === stateFilter);
      }
      const pts = filtered
        .filter(
          (r) =>
            r.latitude != null &&
            r.longitude != null &&
            r[selectedCol] != null &&
            !isNaN(Number(r[selectedCol])),
        )
        .map((r) => ({
          lat: Number(r.latitude),
          lon: Number(r.longitude),
          val: Number(r[selectedCol]),
          name: String(r.city_state ?? r.canonical_id ?? ""),
        }));
      setPoints(pts);
    } catch {
      setPoints([]);
    } finally {
      setLoading(false);
    }
  }, [selectedDs, selectedCol, stateFilter, loadData]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const { minVal, maxVal } = useMemo(() => {
    if (!points.length) return { minVal: 0, maxVal: 1 };
    const vals = points.map((p) => p.val);
    return { minVal: Math.min(...vals), maxVal: Math.max(...vals) };
  }, [points]);

  const bounds = useMemo(() => {
    if (!points.length)
      return { minLat: 30, maxLat: 35, minLon: -88, maxLon: -81 };
    return {
      minLat: Math.min(...points.map((p) => p.lat)),
      maxLat: Math.max(...points.map((p) => p.lat)),
      minLon: Math.min(...points.map((p) => p.lon)),
      maxLon: Math.max(...points.map((p) => p.lon)),
    };
  }, [points]);

  const svgWidth = 700;
  const svgHeight = 420;
  const pad = 20;

  function project(lat: number, lon: number) {
    const xRange = bounds.maxLon - bounds.minLon || 1;
    const yRange = bounds.maxLat - bounds.minLat || 1;
    const x = pad + ((lon - bounds.minLon) / xRange) * (svgWidth - 2 * pad);
    const y =
      pad + ((bounds.maxLat - lat) / yRange) * (svgHeight - 2 * pad);
    return { x, y };
  }

  const [hovered, setHovered] = useState<number | null>(null);

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
          Color by
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
      </div>
      {loading ? (
        <div className="eda-chart-loading">Loading...</div>
      ) : (
        <div className="eda-geo-wrap">
          <svg
            viewBox={`0 0 ${svgWidth} ${svgHeight}`}
            className="eda-geo-svg"
          >
            <rect
              width={svgWidth}
              height={svgHeight}
              fill="#0f1117"
              rx={8}
            />
            {points.map((pt, i) => {
              const { x, y } = project(pt.lat, pt.lon);
              return (
                <circle
                  key={i}
                  cx={x}
                  cy={y}
                  r={hovered === i ? 6 : 4}
                  fill={valueToColor(pt.val, minVal, maxVal)}
                  stroke={hovered === i ? "#fff" : "none"}
                  strokeWidth={1.5}
                  onMouseEnter={() => setHovered(i)}
                  onMouseLeave={() => setHovered(null)}
                />
              );
            })}
            {hovered !== null && points[hovered] && (
              <g>
                <rect
                  x={project(points[hovered].lat, points[hovered].lon).x + 8}
                  y={project(points[hovered].lat, points[hovered].lon).y - 28}
                  width={Math.max(points[hovered].name.length * 6.5 + 60, 120)}
                  height={22}
                  rx={4}
                  fill="#1a1d27"
                  stroke="#2a2e3e"
                />
                <text
                  x={project(points[hovered].lat, points[hovered].lon).x + 14}
                  y={project(points[hovered].lat, points[hovered].lon).y - 13}
                  fill="#e4e6eb"
                  fontSize={11}
                >
                  {points[hovered].name}:{" "}
                  {points[hovered].val.toLocaleString(undefined, {
                    maximumFractionDigits: 2,
                  })}
                </text>
              </g>
            )}
          </svg>
          <div className="eda-geo-legend">
            <span>{minVal.toLocaleString(undefined, { maximumFractionDigits: 1 })}</span>
            <div className="eda-geo-gradient" />
            <span>{maxVal.toLocaleString(undefined, { maximumFractionDigits: 1 })}</span>
          </div>
        </div>
      )}
    </div>
  );
}
