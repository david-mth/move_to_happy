import { useCallback, useEffect, useRef, useState } from "react";
import { Link, Route, Routes, useLocation } from "react-router-dom";
import { fetchMetadata, fetchScore } from "./api";
import { CommunityCard } from "./components/CommunityCard";
import { DataChat } from "./components/DataChat";
import { DataExplorer } from "./components/DataExplorer";
import { PreferencePanel } from "./components/PreferencePanel";
import { ResultsMap } from "./components/ResultsMap";
import type { CommunityScore, Metadata, ScoreRequest, ScoreResponse } from "./types";
import { DEFAULT_PREFS } from "./types";

function ScorerPage() {
  const [prefs, setPrefs] = useState<ScoreRequest>(DEFAULT_PREFS);
  const [result, setResult] = useState<ScoreResponse | null>(null);
  const [meta, setMeta] = useState<Metadata | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<CommunityScore | null>(null);
  const cardsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetchMetadata().then(setMeta).catch(() => {});
  }, []);

  const handleScore = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSelected(null);
    try {
      const data = await fetchScore(prefs);
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [prefs]);

  const handleSelect = useCallback((c: CommunityScore) => {
    setSelected((prev) => (prev?.canonical_id === c.canonical_id ? null : c));
    cardsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, []);

  return (
    <div className="layout">
      <aside className="sidebar">
        <PreferencePanel
          prefs={prefs}
          meta={meta}
          onChange={setPrefs}
          onScore={handleScore}
          loading={loading}
        />
      </aside>

      <main className="content">
        <ResultsMap
          rankings={result?.rankings ?? []}
          anchorLat={prefs.anchor_lat}
          anchorLon={prefs.anchor_lon}
          radiusMiles={prefs.max_radius_miles}
          selected={selected}
          onSelect={handleSelect}
        />

        {error && <div className="error-banner">{error}</div>}

        {result && (
          <div className="results-summary">
            <span>
              <strong>{result.total_candidates}</strong> communities matched
            </span>
            <span className="sep">|</span>
            <span>
              Max purchase price:{" "}
              <strong>${result.max_purchase_price.toLocaleString()}</strong>
            </span>
            <span className="sep">|</span>
            <span>
              Showing top <strong>{result.rankings.length}</strong>
            </span>
          </div>
        )}

        <div ref={cardsRef} className="cards-grid">
          {result?.rankings.map((c, i) => (
            <CommunityCard
              key={c.canonical_id}
              community={c}
              rank={i + 1}
              isSelected={selected?.canonical_id === c.canonical_id}
              onSelect={() => handleSelect(c)}
            />
          ))}
        </div>
      </main>
    </div>
  );
}

export default function App() {
  const location = useLocation();

  return (
    <div className="app">
      <header className="header">
        <div className="header-inner">
          <div>
            <h1>Move to Happy</h1>
            <p className="subtitle">
              Find your ideal community across Georgia, Alabama &amp; Florida
            </p>
          </div>
          <nav className="nav-tabs">
            <Link
              to="/"
              className={`nav-tab ${location.pathname === "/" ? "active" : ""}`}
            >
              Scorer
            </Link>
            <Link
              to="/data"
              className={`nav-tab ${location.pathname === "/data" ? "active" : ""}`}
            >
              Data Explorer
            </Link>
            <Link
              to="/chat"
              className={`nav-tab ${location.pathname === "/chat" ? "active" : ""}`}
            >
              Chat
            </Link>
          </nav>
        </div>
      </header>

      <Routes>
        <Route path="/" element={<ScorerPage />} />
        <Route path="/data" element={<DataExplorer />} />
        <Route path="/chat" element={<DataChat />} />
      </Routes>
    </div>
  );
}
