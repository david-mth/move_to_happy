import { useCallback, useEffect, useRef, useState } from "react";
import { sendConciergeMessage, fetchConciergeStatus } from "../api";

interface Message {
  role: "user" | "assistant";
  content: string;
}

const STARTERS = [
  "I'm looking for a community near Atlanta with a budget of $2,500/month",
  "Help me find an affordable place in Florida near the beach",
  "I want to live near mountains in Georgia with good schools",
  "Compare communities near Birmingham, Alabama for a family of four",
  "I need a quiet community with good healthcare access",
  "Find me a place with low crime and fast internet",
];

function renderMarkdown(text: string) {
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
      // skip blank
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

export function DataChat() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [available, setAvailable] = useState<boolean | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    fetchConciergeStatus().then((s) => setAvailable(s.available));
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

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
        };
        setMessages((prev) => [...prev, assistantMsg]);
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
    [loading],
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
    <div className="chat-page">
      <div className="chat-messages">
        {messages.length === 0 && (
          <div className="chat-welcome">
            <h2>Move to Happy Concierge</h2>
            <p>
              Tell me about your ideal community — your budget, where you want
              to be, and what matters most. I'll find your best matches using
              the Lifestyle Matching Engine and explain why each one fits.
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
          <div key={i} className={`chat-msg chat-msg-${msg.role}`}>
            <div className="chat-msg-label">
              {msg.role === "user" ? "You" : "Concierge"}
            </div>
            <div className="chat-msg-body">
              {msg.role === "assistant" ? (
                renderMarkdown(msg.content)
              ) : (
                <p>{msg.content}</p>
              )}
            </div>
          </div>
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
  );
}
