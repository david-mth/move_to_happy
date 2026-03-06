import { useState } from "react";
import type {
  ConciergeAnchor,
  ConciergeExplanation,
  ConciergeResults,
  ConciergeCommunityScore,
} from "../types";
import { CommunityCard } from "./CommunityCard";
import { ResultsMap } from "./ResultsMap";

interface Props {
  results: ConciergeResults;
  explanations?: ConciergeExplanation[] | null;
  anchor: ConciergeAnchor;
  selectedId: string | null;
  onSelect: (id: string) => void;
}

function ExplanationBlock({
  canonicalId,
  explanations,
}: {
  canonicalId: string;
  explanations?: ConciergeExplanation[] | null;
}) {
  const [open, setOpen] = useState(false);
  const expl = explanations?.find((e) => e.canonical_city_id === canonicalId);
  if (!expl) return null;

  return (
    <div className="concierge-explanation">
      <button
        className="concierge-explanation-toggle"
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
      >
        {open ? "▾" : "▸"} Why this community?
      </button>
      {open && (
        <div className="concierge-explanation-body">
          <p>{expl.explanation}</p>
          {expl.spillover_explanation && (
            <p className="concierge-spillover-note">
              {expl.spillover_explanation}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

export function ConciergeResultsPanel({
  results,
  explanations,
  anchor,
  selectedId,
  onSelect,
}: Props) {
  const selected =
    results.rankings.find((c) => c.canonical_id === selectedId) ?? null;

  const [afHigh] = results.affordability_window;

  return (
    <div className="concierge-results-panel">
      {/* Context header */}
      <div className="concierge-context-header">
        <div className="concierge-header-stats">
          <span>
            <strong>{results.total_candidates}</strong> communities matched
          </span>
          <span className="concierge-header-sep">·</span>
          <span>
            Max purchase{" "}
            <strong>${results.max_purchase_price.toLocaleString()}</strong>
          </span>
          {afHigh > 0 && (
            <>
              <span className="concierge-header-sep">·</span>
              <span>
                Window up to{" "}
                <strong>${afHigh.toLocaleString()}</strong>
              </span>
            </>
          )}
        </div>
        <div className="concierge-header-eliminated">
          {results.eliminated_count} communities outside criteria
        </div>
      </div>

      {/* Map — taller in the side panel */}
      <div className="concierge-map-wrap">
        <ResultsMap
          rankings={results.rankings}
          anchorLat={anchor.lat}
          anchorLon={anchor.lon}
          radiusMiles={anchor.radiusMiles}
          selected={selected}
          onSelect={(c) => onSelect(c.canonical_id)}
        />
      </div>

      {/* Community cards — 2-column grid */}
      <div className="concierge-cards">
        {results.rankings.map((c: ConciergeCommunityScore, i) => (
          <div key={c.canonical_id} className="concierge-card-wrap">
            <CommunityCard
              community={c}
              rank={i + 1}
              isSelected={c.canonical_id === selectedId}
              onSelect={() => onSelect(c.canonical_id)}
            />
            <ExplanationBlock
              canonicalId={c.canonical_id}
              explanations={explanations}
            />
          </div>
        ))}
      </div>
    </div>
  );
}
