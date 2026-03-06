import { useMemo, useState } from "react";
import type {
  ConciergeCommunityScore,
  ConciergeExplanation,
  ConciergeResults,
} from "../types";
import { CommunityCard } from "./CommunityCard";
import { ResultsMap } from "./ResultsMap";

interface Props {
  role: "user" | "assistant";
  content: string;
  results?: ConciergeResults | null;
  explanations?: ConciergeExplanation[] | null;
  needs_clarification?: string[] | null;
  onChipSend?: (text: string) => void;
  onViewResults?: () => void;
}

// Common clarification prompts mapped to display labels
const CLARIFICATION_SUGGESTIONS: Record<string, string[]> = {
  "budget.max_monthly_payment": [
    "Around $1,500/month",
    "Around $2,000/month",
    "Around $2,500/month",
    "Around $3,500/month",
  ],
  "geographic_anchor.city_name": [
    "Near Atlanta",
    "Near Birmingham",
    "Near Jacksonville",
    "Near Tampa",
  ],
  "household.bedbath_bucket": [
    "1-2 bedrooms",
    "3 bedrooms",
    "4+ bedrooms",
  ],
  "household.property_type": ["Single family home", "Any type"],
  "geographic_anchor.radius_miles": [
    "Within 50 miles",
    "Within 100 miles",
    "Within 150 miles",
  ],
};

function getSuggestions(fields: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const field of fields) {
    const suggestions = CLARIFICATION_SUGGESTIONS[field] ?? [];
    for (const s of suggestions) {
      if (!seen.has(s)) {
        seen.add(s);
        out.push(s);
      }
    }
  }
  return out.slice(0, 6);
}

export function renderMarkdown(text: string): React.ReactNode {
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    if (
      line.startsWith("|") &&
      i + 1 < lines.length &&
      lines[i + 1].match(/^\|[-| :]+\|/)
    ) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].startsWith("|")) {
        tableLines.push(lines[i]);
        i++;
      }
      elements.push(renderTable(tableLines, elements.length));
      continue;
    }

    if (line.startsWith("### ")) {
      elements.push(<h4 key={i} className="chat-h3">{line.slice(4)}</h4>);
    } else if (line.startsWith("## ")) {
      elements.push(<h3 key={i} className="chat-h3">{line.slice(3)}</h3>);
    } else if (line.startsWith("# ")) {
      elements.push(<h3 key={i} className="chat-h3">{line.slice(2)}</h3>);
    } else if (line.startsWith("- ") || line.startsWith("* ")) {
      const items: string[] = [];
      while (
        i < lines.length &&
        (lines[i].startsWith("- ") || lines[i].startsWith("* "))
      ) {
        items.push(lines[i].slice(2));
        i++;
      }
      elements.push(
        <ul key={`ul-${elements.length}`} className="chat-list">
          {items.map((item, j) => (
            <li key={j}>{renderInline(item)}</li>
          ))}
        </ul>,
      );
      continue;
    } else if (line.match(/^\d+\. /)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].match(/^\d+\. /)) {
        items.push(lines[i].replace(/^\d+\.\s*/, ""));
        i++;
      }
      elements.push(
        <ol key={`ol-${elements.length}`} className="chat-list">
          {items.map((item, j) => (
            <li key={j}>{renderInline(item)}</li>
          ))}
        </ol>,
      );
      continue;
    } else if (line.trim() === "") {
      // skip blank lines
    } else {
      elements.push(
        <p key={i} className="chat-p">
          {renderInline(line)}
        </p>,
      );
    }
    i++;
  }
  return <>{elements}</>;
}

function renderInline(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return parts.map((part, i) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={i}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={i} className="chat-inline-code">
          {part.slice(1, -1)}
        </code>
      );
    }
    return part;
  });
}

function renderTable(lines: string[], keyBase: number) {
  const headers = lines[0]
    .split("|")
    .filter((c) => c.trim() !== "")
    .map((c) => c.trim());
  const bodyLines = lines.slice(2);
  const rows = bodyLines.map((line) =>
    line
      .split("|")
      .filter((c) => c.trim() !== "")
      .map((c) => c.trim()),
  );
  return (
    <div key={`tbl-${keyBase}`} className="chat-table-wrap">
      <table className="chat-table">
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri}>
              {row.map((cell, ci) => (
                <td key={ci}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Inline results — map + card grid embedded directly in the chat message
// ---------------------------------------------------------------------------

function InlineResults({
  results,
  explanations,
  onViewResults,
}: {
  results: ConciergeResults;
  explanations?: ConciergeExplanation[] | null;
  onViewResults?: () => void;
}) {
  const [selectedId, setSelectedId] = useState<string>(
    results.rankings[0]?.canonical_id ?? "",
  );

  const anchor = useMemo(() => {
    const r = results.rankings;
    if (r.length === 0) return { lat: 33.749, lon: -84.388, radiusMiles: 120 };
    const lat = r.reduce((s, c) => s + c.latitude, 0) / r.length;
    const lon = r.reduce((s, c) => s + c.longitude, 0) / r.length;
    const maxDist = Math.max(...r.map((c) => c.dist_to_anchor ?? 60), 60);
    return { lat, lon, radiusMiles: Math.ceil(maxDist * 1.2) };
  }, [results.rankings]);

  const selected =
    results.rankings.find((c) => c.canonical_id === selectedId) ?? null;

  const [afLow, afHigh] = results.affordability_window;

  return (
    <div className="concierge-inline-results">
      {/* Header strip */}
      <div className="concierge-inline-header">
        <div className="concierge-inline-stats">
          <span>
            <strong>{results.total_candidates}</strong> matched
          </span>
          <span className="concierge-inline-sep">·</span>
          <span>
            Max <strong>${results.max_purchase_price.toLocaleString()}</strong>
          </span>
          {afHigh > 0 && (
            <>
              <span className="concierge-inline-sep">·</span>
              <span>
                Window{" "}
                <strong>
                  ${afLow.toLocaleString()}–${afHigh.toLocaleString()}
                </strong>
              </span>
            </>
          )}
          <span className="concierge-inline-sep">·</span>
          <span className="concierge-inline-eliminated">
            {results.eliminated_count} outside criteria
          </span>
        </div>
        {onViewResults && (
          <button className="concierge-view-map-btn" onClick={onViewResults}>
            Full panel ↗
          </button>
        )}
      </div>

      {/* Map */}
      <div className="concierge-inline-map-wrap">
        <ResultsMap
          rankings={results.rankings}
          anchorLat={anchor.lat}
          anchorLon={anchor.lon}
          radiusMiles={anchor.radiusMiles}
          selected={selected}
          onSelect={(c) => setSelectedId(c.canonical_id)}
        />
      </div>

      {/* Card grid */}
      <div className="concierge-inline-cards-grid">
        {results.rankings.map((c: ConciergeCommunityScore, i) => (
          <div key={c.canonical_id} className="concierge-inline-card-wrap">
            <CommunityCard
              community={c}
              rank={i + 1}
              isSelected={c.canonical_id === selectedId}
              onSelect={() => setSelectedId(c.canonical_id)}
            />
            {explanations?.find((e) => e.canonical_city_id === c.canonical_id) && (
              <InlineExplanation
                canonicalId={c.canonical_id}
                explanations={explanations}
              />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function InlineExplanation({
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
            <p className="concierge-spillover-note">{expl.spillover_explanation}</p>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main message component
// ---------------------------------------------------------------------------

export function ConciergeMessage({
  role,
  content,
  results,
  explanations,
  needs_clarification,
  onChipSend,
  onViewResults,
}: Props) {
  const chips =
    role === "assistant" && needs_clarification?.length
      ? getSuggestions(needs_clarification)
      : [];

  const showResults =
    role === "assistant" && results && results.rankings.length > 0;

  return (
    <div className={`chat-msg chat-msg-${role}`}>
      <div className="chat-msg-label">
        {role === "user" ? "You" : "Concierge"}
      </div>
      <div className="chat-msg-body">
        {role === "assistant" ? renderMarkdown(content) : <p>{content}</p>}

        {showResults && (
          <InlineResults
            results={results}
            explanations={explanations}
            onViewResults={onViewResults}
          />
        )}
      </div>

      {/* Clarification quick-reply chips */}
      {chips.length > 0 && onChipSend && (
        <div className="concierge-clarification-chips">
          {chips.map((chip) => (
            <button
              key={chip}
              className="concierge-chip"
              onClick={() => onChipSend(chip)}
            >
              {chip}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
