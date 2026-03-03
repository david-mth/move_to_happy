import { useCallback, useEffect, useRef, useState } from "react";
import { sendChatMessage, fetchChatStatus } from "../api";
import type { ChatMessage } from "../types";

const STARTERS = [
  "What are the safest communities in Georgia?",
  "Which communities have the best broadband access?",
  "Compare average home prices across the three states",
  "What are the top 10 communities by median household income?",
  "Which communities have the most hospitals within 30 miles?",
  "Show me communities with low crime rates and good air quality",
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

export function DataChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [available, setAvailable] = useState<boolean | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    fetchChatStatus().then((s) => setAvailable(s.available));
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const buildHistory = useCallback(() => {
    const hist: { role: string; content: string }[] = [];
    for (const msg of messages) {
      hist.push({ role: msg.role, content: msg.content });
    }
    return hist;
  }, [messages]);

  const send = useCallback(
    async (text: string) => {
      if (!text.trim() || loading) return;

      const userMsg: ChatMessage = { role: "user", content: text, table: null };
      setMessages((prev) => [...prev, userMsg]);
      setInput("");
      setLoading(true);

      try {
        const history = buildHistory();
        const res = await sendChatMessage(text, history);
        const assistantMsg: ChatMessage = {
          role: "assistant",
          content: res.content,
          table: res.table,
        };
        setMessages((prev) => [...prev, assistantMsg]);
      } catch (e) {
        const errMsg: ChatMessage = {
          role: "assistant",
          content:
            e instanceof Error
              ? `Error: ${e.message}`
              : "Something went wrong. Please try again.",
          table: null,
        };
        setMessages((prev) => [...prev, errMsg]);
      } finally {
        setLoading(false);
        inputRef.current?.focus();
      }
    },
    [loading, buildHistory],
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
          <h2>Chat Unavailable</h2>
          <p>
            The ANTHROPIC_API_KEY environment variable is not set. Set it in
            Replit Secrets or a local .env file to enable the chat feature.
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
            <h2>Chat with your data</h2>
            <p>
              Ask questions about your community datasets in plain English.
              Claude will analyze the data and respond with insights.
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
              {msg.role === "user" ? "You" : "Claude"}
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
            <div className="chat-msg-label">Claude</div>
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
          placeholder="Ask a question about your data..."
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
