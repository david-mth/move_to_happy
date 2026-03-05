import { useCallback, useEffect, useRef, useState } from "react";
import { sendConciergeMessage, fetchConciergeStatus } from "../api";
import type {
  ConciergeAnchor,
  ConciergeExplanation,
  ConciergeResults,
} from "../types";
import { ConciergeMessage } from "./ConciergeMessage";
import { ConciergeResultsPanel } from "./ConciergeResultsPanel";

interface Message {
  role: "user" | "assistant";
  content: string;
  results?: ConciergeResults | null;
  explanations?: ConciergeExplanation[] | null;
  needs_clarification?: string[] | null;
}

const DEFAULT_ANCHOR: ConciergeAnchor = {
  lat: 33.749,
  lon: -84.388,
  radiusMiles: 120,
};

const STARTERS = [
  "Help me find a community near Atlanta with a $2,500/month budget",
  "Find affordable places in Florida near the beach",
  "I want mountain views in Georgia — budget around $2,000/month",
  "Looking for a safe, family-friendly community near Birmingham",
  "Find a quiet coastal community in Florida under $3,000/month",
  "I need good healthcare access near a city in Georgia or Alabama",
];

export function DataChat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [available, setAvailable] = useState<boolean | null>(null);

  // Results panel state
  const [activeResults, setActiveResults] = useState<ConciergeResults | null>(null);
  const [activeExplanations, setActiveExplanations] = useState<
    ConciergeExplanation[] | null
  >(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [anchor, setAnchor] = useState<ConciergeAnchor>(DEFAULT_ANCHOR);
  const [isPanelOpen, setIsPanelOpen] = useState(false);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    fetchConciergeStatus().then((s) => setAvailable(s.available));
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Try to extract anchor lat/lon from LME results for the map
  const extractAnchor = useCallback(
    (results: ConciergeResults): ConciergeAnchor => {
      // Estimate anchor from the centroid of ranked communities
      if (results.rankings.length === 0) return anchor;
      const lats = results.rankings.map((c) => c.latitude);
      const lons = results.rankings.map((c) => c.longitude);
      const centerLat = lats.reduce((a, b) => a + b, 0) / lats.length;
      const centerLon = lons.reduce((a, b) => a + b, 0) / lons.length;
      // Estimate radius from max dist_to_anchor
      const maxDist = Math.max(
        ...results.rankings.map((c) => c.dist_to_anchor ?? 120),
        60,
      );
      return { lat: centerLat, lon: centerLon, radiusMiles: Math.ceil(maxDist * 1.2) };
    },
    [anchor],
  );

  const send = useCallback(
    async (text: string) => {
      if (!text.trim() || loading) return;

      const userMsg: Message = { role: "user", content: text };
      setMessages((prev) => [...prev, userMsg]);
      setInput("");
      setLoading(true);

      try {
        const res = await sendConciergeMessage(text);

        const assistantMsg: Message = {
          role: "assistant",
          content: res.content,
          results: res.results,
          explanations: res.explanations,
          needs_clarification: res.needs_clarification,
        };
        setMessages((prev) => [...prev, assistantMsg]);

        // If we got LME results, activate the results panel
        if (res.results && res.results.rankings.length > 0) {
          const newAnchor = extractAnchor(res.results);
          setAnchor(newAnchor);
          setActiveResults(res.results);
          setActiveExplanations(res.explanations ?? null);
          setSelectedId(res.results.rankings[0]?.canonical_id ?? null);
          setIsPanelOpen(true);
        }
      } catch (e) {
        const errMsg: Message = {
          role: "assistant",
          content:
            e instanceof Error
              ? `Error: ${e.message}`
              : "Something went wrong. Please try again.",
        };
        setMessages((prev) => [...prev, errMsg]);
      } finally {
        setLoading(false);
        inputRef.current?.focus();
      }
    },
    [loading, extractAnchor],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send(input);
      }
    },
    [input, send],
  );

  if (available === false) {
    return (
      <div className="chat-page">
        <div className="chat-unavailable">
          <h2>Concierge Unavailable</h2>
          <p>
            The ANTHROPIC_API_KEY environment variable is not set. Set it in
            your .env file to enable the AI concierge.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={`concierge-layout${isPanelOpen ? " panel-open" : ""}`}>
      {/* Left: conversation thread */}
      <div className="concierge-thread">
        <div className="chat-messages">
          {messages.length === 0 && (
            <div className="chat-welcome">
              <h2>Move to Happy Concierge</h2>
              <p>
                Tell me your budget, where you want to be, and what matters
                most. I'll match you against{" "}
                <strong>1,305+ communities</strong> in Georgia, Alabama &amp;
                Florida and explain exactly why each one fits.
              </p>
              <div className="chat-starters">
                {STARTERS.map((q) => (
                  <button
                    key={q}
                    className="chat-starter"
                    onClick={() => send(q)}
                    disabled={loading}
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg, i) => (
            <ConciergeMessage
              key={i}
              role={msg.role}
              content={msg.content}
              results={msg.results}
              explanations={msg.explanations}
              needs_clarification={msg.needs_clarification}
              onChipSend={send}
              onViewResults={
                msg.results
                  ? () => {
                      setIsPanelOpen(true);
                      if (msg.results) {
                        setActiveResults(msg.results);
                        setActiveExplanations(msg.explanations ?? null);
                      }
                    }
                  : undefined
              }
            />
          ))}

          {loading && (
            <div className="chat-msg chat-msg-assistant">
              <div className="chat-msg-label">Concierge</div>
              <div className="chat-msg-body">
                <div className="chat-thinking">
                  <span className="dot" />
                  <span className="dot" />
                  <span className="dot" />
                </div>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        <div className="chat-input-area">
          {isPanelOpen && activeResults && (
            <button
              className="concierge-panel-toggle"
              onClick={() => setIsPanelOpen((v) => !v)}
              title="Toggle results panel"
            >
              {isPanelOpen ? "Hide results ✕" : "Show results ↗"}
            </button>
          )}
          <textarea
            ref={inputRef}
            className="chat-input"
            placeholder="Tell me about your ideal community..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={1}
            disabled={loading}
          />
          <button
            className="chat-send"
            onClick={() => send(input)}
            disabled={loading || !input.trim()}
          >
            Send
          </button>
        </div>
      </div>

      {/* Right: results panel */}
      {isPanelOpen && activeResults && (
        <ConciergeResultsPanel
          results={activeResults}
          explanations={activeExplanations}
          anchor={anchor}
          selectedId={selectedId}
          onSelect={setSelectedId}
        />
      )}
    </div>
  );
}
