import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { CircleMarker, MapContainer, Popup, TileLayer, useMap } from "react-leaflet";
type Row = Record<string, unknown>;

interface Props {
  loadData: (dataset: string, columns: string[]) => Promise<Row[]>;
}

const STATE_COLORS: Record<string, string> = {
  Georgia: "#6c63ff",
  Alabama: "#22c55e",
  Florida: "#eab308",
};

function FitBounds({ points }: { points: { lat: number; lon: number }[] }) {
  const map = useMap();
  useEffect(() => {
    if (!points.length) return;
    const latlngs: L.LatLngExpression[] = points.map((p) => [p.lat, p.lon]);
    map.fitBounds(L.latLngBounds(latlngs), { padding: [30, 30] });
  }, [points, map]);
  return null;
}

export function SafetyHealth({ loadData }: Props) {
  const [crimeData, setCrimeData] = useState<Row[]>([]);
  const [censusData, setCensusData] = useState<Row[]>([]);
  const [hospitalData, setHospitalData] = useState<Row[]>([]);
  const [physicianData, setPhysicianData] = useState<Row[]>([]);
  const [commData, setCommData] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [crimeDir, setCrimeDir] = useState<"safest" | "dangerous">("safest");

  useEffect(() => {
    setLoading(true);
    Promise.all([
      loadData("crime", ["canonical_id", "violent_crime_rate", "property_crime_rate", "state_name"]),
      loadData("census", ["canonical_id", "median_household_income", "state_name"]),
      loadData("hospitals", [
        "canonical_id", "nearest_hospital_miles", "hospitals_within_30mi",
        "avg_rating_within_30mi", "state_name",
      ]),
      loadData("physicians", ["canonical_id", "providers_per_1000_pop", "total_providers", "state_name"]),
      loadData("communities", ["canonical_id", "city_state", "latitude", "longitude", "state_name", "population"]),
    ])
      .then(([cr, ce, ho, ph, co]) => {
        setCrimeData(cr);
        setCensusData(ce);
        setHospitalData(ho);
        setPhysicianData(ph);
        setCommData(co);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [loadData]);

  const crimeRanked = useMemo(() => {
    const commMap = new Map(commData.map((r) => [r.canonical_id, r]));
    const items = crimeData
      .filter((r) => r.violent_crime_rate != null && !isNaN(Number(r.violent_crime_rate)))
      .map((r) => {
        const comm = commMap.get(r.canonical_id);
        return {
          name: String(comm?.city_state ?? r.canonical_id),
          state: String(r.state_name ?? ""),
          rate: Number(r.violent_crime_rate),
        };
      });
    items.sort((a, b) => crimeDir === "safest" ? a.rate - b.rate : b.rate - a.rate);
    return items.slice(0, 15);
  }, [crimeData, commData, crimeDir]);

  const crimeIncomeScatter = useMemo(() => {
    const censusMap = new Map(censusData.map((r) => [r.canonical_id, r]));
    const commMap = new Map(commData.map((r) => [r.canonical_id, r]));
    return crimeData
      .filter((r) => {
        const c = censusMap.get(r.canonical_id);
        return c && c.median_household_income != null && r.violent_crime_rate != null;
      })
      .map((r) => {
        const c = censusMap.get(r.canonical_id)!;
        const comm = commMap.get(r.canonical_id);
        return {
          x: Number(c.median_household_income),
          y: Number(r.violent_crime_rate),
          name: String(comm?.city_state ?? r.canonical_id),
          state: String(r.state_name ?? ""),
        };
      });
  }, [crimeData, censusData, commData]);

  const scatterGrouped: Record<string, typeof crimeIncomeScatter> = {};
  for (const pt of crimeIncomeScatter) {
    (scatterGrouped[pt.state] ??= []).push(pt);
  }

  const healthcareMapData = useMemo(() => {
    const commMap = new Map(commData.map((r) => [r.canonical_id, r]));
    const physMap = new Map(physicianData.map((r) => [r.canonical_id, r]));
    return hospitalData
      .filter((r) => {
        const comm = commMap.get(r.canonical_id);
        return comm && comm.latitude != null && comm.longitude != null;
      })
      .map((r) => {
        const comm = commMap.get(r.canonical_id)!;
        const phys = physMap.get(r.canonical_id);
        return {
          lat: Number(comm.latitude),
          lon: Number(comm.longitude),
          name: String(comm.city_state),
          providers: Number(phys?.providers_per_1000_pop ?? 0),
          hospitals: Number(r.hospitals_within_30mi ?? 0),
          rating: Number(r.avg_rating_within_30mi ?? 0),
          distance: Number(r.nearest_hospital_miles ?? 0),
        };
      });
  }, [hospitalData, physicianData, commData]);

  const hospitalDistances = useMemo(() => {
    const vals = hospitalData
      .map((r) => Number(r.nearest_hospital_miles))
      .filter((v) => !isNaN(v) && v > 0);
    if (!vals.length) return { histogram: [], mean: 0, median: 0, p90: 0 };
    vals.sort((a, b) => a - b);
    const bins = 20;
    const min = vals[0];
    const max = vals[vals.length - 1];
    const step = (max - min) / bins || 1;
    const buckets = Array.from({ length: bins }, (_, i) => ({
      range: `${(min + i * step).toFixed(0)}-${(min + (i + 1) * step).toFixed(0)}`,
      count: 0,
    }));
    for (const v of vals) {
      const idx = Math.min(Math.floor((v - min) / step), bins - 1);
      buckets[idx].count++;
    }
    const mid = Math.floor(vals.length / 2);
    return {
      histogram: buckets,
      mean: vals.reduce((a, b) => a + b, 0) / vals.length,
      median: vals.length % 2 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2,
      p90: vals[Math.floor(vals.length * 0.9)],
    };
  }, [hospitalData]);

  if (loading) {
    return <div className="eda-chart-loading">Loading safety & health data...</div>;
  }

  return (
    <div className="eda-safety">
      {/* Crime Ranking */}
      <div className="eda-section">
        <div className="eda-section-header">
          <h3 className="eda-section-title">
            {crimeDir === "safest" ? "Safest" : "Most Dangerous"} Communities (Violent Crime Rate)
          </h3>
          <div className="eda-toggle">
            <button className={crimeDir === "safest" ? "active" : ""} onClick={() => setCrimeDir("safest")}>Safest</button>
            <button className={crimeDir === "dangerous" ? "active" : ""} onClick={() => setCrimeDir("dangerous")}>Highest Crime</button>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={Math.max(300, crimeRanked.length * 28)}>
          <BarChart data={crimeRanked} layout="vertical" margin={{ top: 5, right: 20, bottom: 5, left: 140 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" horizontal={false} />
            <XAxis type="number" tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <YAxis type="category" dataKey="name" tick={{ fill: "#8b8fa3", fontSize: 11 }} width={130} />
            <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }} />
            <Bar dataKey="rate" name="Violent Crime /100k" radius={[0, 4, 4, 0]}>
              {crimeRanked.map((entry, idx) => (
                <Cell key={idx} fill={STATE_COLORS[entry.state] ?? "#8b8fa3"} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Crime vs Income Scatter */}
      <div className="eda-section">
        <h3 className="eda-section-title">Crime Rate vs. Median Income</h3>
        <p className="eda-section-desc">Explores the relationship between income levels and violent crime rates.</p>
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
              dataKey="x" type="number" name="Income"
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: "Median Household Income ($)", position: "insideBottom", offset: -10, fill: "#8b8fa3", fontSize: 12 }}
            />
            <YAxis
              dataKey="y" type="number" name="Crime Rate"
              tick={{ fill: "#8b8fa3", fontSize: 11 }}
              label={{ value: "Violent Crime /100k", angle: -90, position: "insideLeft", fill: "#8b8fa3", fontSize: 12 }}
            />
            <ZAxis range={[20, 20]} />
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

      {/* Healthcare Map */}
      <div className="eda-section">
        <h3 className="eda-section-title">Healthcare Access Map</h3>
        <p className="eda-section-desc">Circle color = providers per 1,000 pop (green = more). Size = hospitals within 30 mi.</p>
        <div className="eda-map-container" style={{ height: 400 }}>
          <MapContainer center={[32.7, -84.4]} zoom={6} style={{ height: "100%", width: "100%" }} scrollWheelZoom>
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            {healthcareMapData.map((pt, i) => {
              const provMax = 20;
              const t = Math.min(pt.providers / provMax, 1);
              const color = t > 0.5 ? "#22c55e" : t > 0.25 ? "#eab308" : "#ef4444";
              const radius = Math.max(4, Math.min(14, pt.hospitals * 1.5));
              return (
                <CircleMarker
                  key={i}
                  center={[pt.lat, pt.lon]}
                  radius={radius}
                  pathOptions={{ color, fillColor: color, fillOpacity: 0.7, weight: 1 }}
                >
                  <Popup>
                    <div style={{ fontFamily: "inherit", minWidth: 160, fontSize: 12 }}>
                      <strong>{pt.name}</strong><br />
                      Providers/1k: {pt.providers.toFixed(1)}<br />
                      Hospitals (30mi): {pt.hospitals}<br />
                      Avg Rating: {pt.rating.toFixed(1)}<br />
                      Nearest: {pt.distance.toFixed(1)} mi
                    </div>
                  </Popup>
                </CircleMarker>
              );
            })}
            <FitBounds points={healthcareMapData} />
          </MapContainer>
        </div>
      </div>

      {/* Hospital Distance Distribution */}
      <div className="eda-section">
        <h3 className="eda-section-title">Distance to Nearest Hospital</h3>
        <div className="eda-stat-cards" style={{ marginBottom: "0.75rem" }}>
          <div className="eda-stat-card">
            <div className="eda-stat-value">{hospitalDistances.mean.toFixed(1)} mi</div>
            <div className="eda-stat-label">Mean</div>
          </div>
          <div className="eda-stat-card">
            <div className="eda-stat-value">{hospitalDistances.median.toFixed(1)} mi</div>
            <div className="eda-stat-label">Median</div>
          </div>
          <div className="eda-stat-card">
            <div className="eda-stat-value">{hospitalDistances.p90.toFixed(1)} mi</div>
            <div className="eda-stat-label">90th Percentile</div>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={hospitalDistances.histogram} margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2e3e" />
            <XAxis dataKey="range" tick={{ fill: "#8b8fa3", fontSize: 10 }} angle={-35} textAnchor="end" interval={2} />
            <YAxis tick={{ fill: "#8b8fa3", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#1a1d27", border: "1px solid #2a2e3e", borderRadius: 8, fontSize: 12 }} />
            <Bar dataKey="count" fill="#3b82f6" radius={[3, 3, 0, 0]} name="Communities" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
