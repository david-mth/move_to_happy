import type {
  ConciergeExplanation,
  ConciergeResults,
} from "../types";

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

export function ConciergeMessage({
  role,
  content,
  results,
  needs_clarification,
  onChipSend,
  onViewResults,
}: Props) {
  const chips =
    role === "assistant" && needs_clarification?.length
      ? getSuggestions(needs_clarification)
      : [];

  return (
    <div className={`chat-msg chat-msg-${role}`}>
      <div className="chat-msg-label">
        {role === "user" ? "You" : "Concierge"}
      </div>
      <div className="chat-msg-body">
        {role === "assistant" ? renderMarkdown(content) : <p>{content}</p>}

        {/* Inline results summary — appears inside the message bubble */}
        {role === "assistant" && results && results.rankings.length > 0 && (
          <div className="concierge-inline-summary">
            <span className="concierge-summary-stat">
              <strong>{results.total_candidates}</strong> communities matched
            </span>
            <span className="concierge-summary-sep">·</span>
            <span className="concierge-summary-stat">
              Max purchase{" "}
              <strong>${results.max_purchase_price.toLocaleString()}</strong>
            </span>
            <span className="concierge-summary-sep">·</span>
            <span className="concierge-summary-stat">
              Top <strong>{results.rankings.length}</strong> shown
            </span>
            {onViewResults && (
              <button
                className="concierge-view-map-btn"
                onClick={onViewResults}
              >
                View on map ↗
              </button>
            )}
          </div>
        )}
      </div>

      {/* Clarification quick-reply chips — outside the bubble, below */}
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
