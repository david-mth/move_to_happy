import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { useEffect, useMemo } from "react";
import { Circle, CircleMarker, MapContainer, Popup, TileLayer, useMap } from "react-leaflet";
import type { CommunityScore } from "../types";

interface Props {
  rankings: CommunityScore[];
  anchorLat: number;
  anchorLon: number;
  radiusMiles: number;
  selected: CommunityScore | null;
  onSelect: (c: CommunityScore) => void;
}

function scoreColor(score: number): string {
  if (score >= 0.7) return "#22c55e";
  if (score >= 0.5) return "#eab308";
  if (score >= 0.3) return "#f97316";
  return "#ef4444";
}

function FitBounds({ rankings, anchorLat, anchorLon }: {
  rankings: CommunityScore[];
  anchorLat: number;
  anchorLon: number;
}) {
  const map = useMap();

  useEffect(() => {
    if (rankings.length === 0) return;
    const points: L.LatLngExpression[] = rankings.map((r) => [r.latitude, r.longitude]);
    points.push([anchorLat, anchorLon]);
    const bounds = L.latLngBounds(points);
    map.fitBounds(bounds, { padding: [40, 40] });
  }, [rankings, anchorLat, anchorLon, map]);

  return null;
}

const MILES_TO_METERS = 1609.34;

export function ResultsMap({
  rankings,
  anchorLat,
  anchorLon,
  radiusMiles,
  selected,
  onSelect,
}: Props) {
  const center = useMemo<L.LatLngExpression>(
    () => [anchorLat, anchorLon],
    [anchorLat, anchorLon],
  );

  return (
    <div className="map-container">
      <MapContainer
        center={center}
        zoom={7}
        style={{ height: "100%", width: "100%" }}
        scrollWheelZoom
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        />

        <Circle
          center={center}
          radius={radiusMiles * MILES_TO_METERS}
          pathOptions={{
            color: "#6c63ff",
            fillColor: "#6c63ff",
            fillOpacity: 0.06,
            weight: 1,
            dashArray: "6 4",
          }}
        />

        {/* Anchor marker */}
        <CircleMarker
          center={center}
          radius={8}
          pathOptions={{ color: "#fff", fillColor: "#6c63ff", fillOpacity: 1, weight: 2 }}
        >
          <Popup>
            <strong>Anchor Point</strong>
          </Popup>
        </CircleMarker>

        {rankings.map((c) => {
          const isSelected = selected?.canonical_id === c.canonical_id;
          return (
            <CircleMarker
              key={c.canonical_id}
              center={[c.latitude, c.longitude]}
              radius={isSelected ? 10 : 7}
              pathOptions={{
                color: isSelected ? "#fff" : scoreColor(c.final_score),
                fillColor: scoreColor(c.final_score),
                fillOpacity: 0.85,
                weight: isSelected ? 2 : 1,
              }}
              eventHandlers={{ click: () => onSelect(c) }}
            >
              <Popup>
                <div style={{ fontFamily: "inherit", minWidth: 160 }}>
                  <strong>{c.city_state}</strong>
                  <br />
                  Score: {c.final_score.toFixed(3)}
                  <br />
                  ${c.median_home_price.toLocaleString()} median
                  <br />
                  Pop: {c.population.toLocaleString()}
                </div>
              </Popup>
            </CircleMarker>
          );
        })}

        <FitBounds rankings={rankings} anchorLat={anchorLat} anchorLon={anchorLon} />
      </MapContainer>
    </div>
  );
}
