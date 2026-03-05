import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useState } from "react";
import { CircleMarker, MapContainer, Popup, TileLayer, useMap } from "react-leaflet";
import type { CommunityIndex } from "../../types";

interface Props {
  indices: CommunityIndex[];
  stateFilter: string;
}

const INDEX_KEYS = [
  "affordability", "safety", "healthcare", "education", "digital", "environmental", "recreation",
] as const;

type MetricKey = (typeof INDEX_KEYS)[number] | "population" | "cost_of_living";

const METRIC_OPTIONS: { value: MetricKey; label: string }[] = [
  { value: "affordability", label: "Affordability Index" },
  { value: "safety", label: "Safety Index" },
  { value: "healthcare", label: "Healthcare Index" },
  { value: "education", label: "Education Index" },
  { value: "digital", label: "Digital Readiness" },
  { value: "environmental", label: "Environmental Index" },
  { value: "recreation", label: "Recreation Index" },
  { value: "population", label: "Population" },
];

function metricColor(val: number, min: number, max: number): string {
  if (max === min) return "#6c63ff";
  const t = Math.max(0, Math.min(1, (val - min) / (max - min)));
  if (t < 0.25) return "#ef4444";
  if (t < 0.5) return "#f97316";
  if (t < 0.75) return "#eab308";
  return "#22c55e";
}

function FitBounds({ points }: { points: CommunityIndex[] }) {
  const map = useMap();
  useEffect(() => {
    if (!points.length) return;
    const latlngs: L.LatLngExpression[] = points
      .filter((p) => p.latitude && p.longitude)
      .map((p) => [p.latitude, p.longitude]);
    if (latlngs.length) {
      map.fitBounds(L.latLngBounds(latlngs), { padding: [30, 30] });
    }
  }, [points, map]);
  return null;
}

export function CommunityOverview({ indices, stateFilter }: Props) {
  const [metric, setMetric] = useState<MetricKey>("affordability");
  const [rankMetric, setRankMetric] = useState<MetricKey>("affordability");
  const [rankDir, setRankDir] = useState<"top" | "bottom">("top");
  const [search, setSearch] = useState("");
  const [rankCount, setRankCount] = useState(20);

  const filtered = useMemo(() => {
    if (stateFilter === "All") return indices;
    return indices.filter((c) => c.state_name === stateFilter);
  }, [indices, stateFilter]);

  const { min, max } = useMemo(() => {
    const vals = filtered
      .map((c) => {
        const v = c[metric as keyof CommunityIndex];
        return typeof v === "number" ? v : null;
      })
      .filter((v): v is number => v != null);
    return {
      min: vals.length ? Math.min(...vals) : 0,
      max: vals.length ? Math.max(...vals) : 100,
    };
  }, [filtered, metric]);

  const ranked = useMemo(() => {
    let items = filtered.filter((c) => {
      const v = c[rankMetric as keyof CommunityIndex];
      return typeof v === "number" && v != null;
    });
    if (search) {
      const q = search.toLowerCase();
      items = items.filter((c) => c.city_state.toLowerCase().includes(q));
    }
    items.sort((a, b) => {
      const va = (a[rankMetric as keyof CommunityIndex] as number) ?? 0;
      const vb = (b[rankMetric as keyof CommunityIndex] as number) ?? 0;
      return rankDir === "top" ? vb - va : va - vb;
    });
    return items.slice(0, rankCount);
  }, [filtered, rankMetric, rankDir, search, rankCount]);

  const topVal = ranked[0]
    ? Math.abs((ranked[0][rankMetric as keyof CommunityIndex] as number) ?? 1)
    : 1;

  return (
    <div className="eda-overview-layout">
      <div className="eda-overview-map">
        <div className="eda-controls" style={{ marginBottom: "0.75rem" }}>
          <label>
            Color by
            <select value={metric} onChange={(e) => setMetric(e.target.value as MetricKey)}>
              {METRIC_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </label>
        </div>
        <div className="eda-map-container">
          <MapContainer
            center={[32.7, -84.4]}
            zoom={6}
            style={{ height: "100%", width: "100%" }}
            scrollWheelZoom
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            {filtered.map((c) => {
              const val = c[metric as keyof CommunityIndex];
              if (typeof val !== "number" || !c.latitude || !c.longitude) return null;
              const pop = c.population ?? 1000;
              const radius = Math.max(4, Math.min(14, Math.sqrt(pop / 1000) * 2));
              return (
                <CircleMarker
                  key={c.canonical_id}
                  center={[c.latitude, c.longitude]}
                  radius={radius}
                  pathOptions={{
                    color: metricColor(val, min, max),
                    fillColor: metricColor(val, min, max),
                    fillOpacity: 0.75,
                    weight: 1,
                  }}
                >
                  <Popup>
                    <div style={{ fontFamily: "inherit", minWidth: 180, fontSize: 12 }}>
                      <strong>{c.city_state}</strong>
                      <br />
                      Pop: {c.population?.toLocaleString() ?? "—"}
                      <br />
                      Affordability: {c.affordability ?? "—"}
                      <br />
                      Safety: {c.safety ?? "—"}
                      <br />
                      Healthcare: {c.healthcare ?? "—"}
                      <br />
                      Education: {c.education ?? "—"}
                      <br />
                      Digital: {c.digital ?? "—"}
                      <br />
                      Environmental: {c.environmental ?? "—"}
                      <br />
                      Recreation: {c.recreation ?? "—"}
                    </div>
                  </Popup>
                </CircleMarker>
              );
            })}
            <FitBounds points={filtered} />
          </MapContainer>
        </div>
        <div className="eda-map-legend">
          <span className="eda-map-legend-dot" style={{ background: "#ef4444" }} /> Low
          <span className="eda-map-legend-dot" style={{ background: "#f97316" }} />
          <span className="eda-map-legend-dot" style={{ background: "#eab308" }} />
          <span className="eda-map-legend-dot" style={{ background: "#22c55e" }} /> High
        </div>
      </div>

      <div className="eda-overview-ranking">
        <div className="eda-controls">
          <label>
            Rank by
            <select value={rankMetric} onChange={(e) => setRankMetric(e.target.value as MetricKey)}>
              {METRIC_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </label>
          <label>
            Direction
            <div className="eda-toggle">
              <button className={rankDir === "top" ? "active" : ""} onClick={() => setRankDir("top")}>Top</button>
              <button className={rankDir === "bottom" ? "active" : ""} onClick={() => setRankDir("bottom")}>Bottom</button>
            </div>
          </label>
        </div>
        <input
          className="eda-rank-search"
          placeholder="Search communities..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <div className="eda-ranking-wrap">
          <table className="eda-ranking-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Community</th>
                <th>State</th>
                <th>{METRIC_OPTIONS.find((o) => o.value === rankMetric)?.label ?? rankMetric}</th>
                <th style={{ width: 140 }}></th>
              </tr>
            </thead>
            <tbody>
              {ranked.map((r, i) => {
                const val = (r[rankMetric as keyof CommunityIndex] as number) ?? 0;
                const pct = topVal ? Math.abs(val / topVal) * 100 : 0;
                return (
                  <tr key={r.canonical_id}>
                    <td className="eda-rank-num">{i + 1}</td>
                    <td className="eda-rank-name">{r.city_state}</td>
                    <td className="eda-rank-state">
                      <span className={`eda-state-badge ${r.state_name.toLowerCase().replace(/\s/g, "-")}`}>
                        {r.state_name.slice(0, 2).toUpperCase()}
                      </span>
                    </td>
                    <td className="eda-rank-value">
                      {val.toLocaleString(undefined, { maximumFractionDigits: 1 })}
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
      </div>
    </div>
  );
}
