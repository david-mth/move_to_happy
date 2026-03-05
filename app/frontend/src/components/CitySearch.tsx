import { useCallback, useEffect, useRef, useState } from "react";

interface CitySearchProps {
  onSelect: (location: { lat: number; lon: number; state: string }) => void;
}

interface NominatimResult {
  display_name: string;
  lat: string;
  lon: string;
}

const ALLOWED_STATES = ["Georgia", "Alabama", "Florida"];

function extractState(displayName: string): string {
  for (const state of ALLOWED_STATES) {
    if (displayName.includes(state)) return state;
  }
  return "";
}

export function CitySearch({ onSelect }: CitySearchProps) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<NominatimResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [highlighted, setHighlighted] = useState(-1);

  const containerRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const search = useCallback(async (q: string) => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    try {
      const params = new URLSearchParams({
        format: "json",
        countrycodes: "us",
        limit: "5",
        q,
        viewbox: "-88.5,35.5,-80.0,25.0",
        bounded: "1",
      });
      const res = await fetch(
        `https://nominatim.openstreetmap.org/search?${params}`,
        {
          signal: controller.signal,
          headers: { "User-Agent": "MoveToHappy/1.0" },
        },
      );
      const data: NominatimResult[] = await res.json();
      const filtered = data.filter((r) =>
        ALLOWED_STATES.some((s) => r.display_name.includes(s)),
      );
      setResults(filtered);
      setHighlighted(-1);
      setOpen(true);
    } catch (e) {
      if ((e as Error).name !== "AbortError") {
        setResults([]);
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (query.length < 3) {
      setResults([]);
      setOpen(false);
      return;
    }
    debounceRef.current = setTimeout(() => {
      search(query);
    }, 400);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [query, search]);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleSelect(result: NominatimResult) {
    const state = extractState(result.display_name);
    onSelect({ lat: parseFloat(result.lat), lon: parseFloat(result.lon), state });
    setQuery("");
    setResults([]);
    setOpen(false);
    setHighlighted(-1);
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (!open || results.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlighted((h) => Math.min(h + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlighted((h) => Math.max(h - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (highlighted >= 0) handleSelect(results[highlighted]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  }

  const showDropdown = open && query.length >= 3;

  return (
    <div className="city-search" ref={containerRef}>
      <input
        className="city-search-input"
        type="text"
        placeholder="Search city or town..."
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        onFocus={() => { if (results.length > 0) setOpen(true); }}
        autoComplete="off"
      />
      {showDropdown && (
        <div className="city-search-results">
          {loading && <div className="city-search-loading">Searching...</div>}
          {!loading && results.length === 0 && (
            <div className="city-search-empty">No results in GA, AL, or FL</div>
          )}
          {!loading &&
            results.map((r, i) => {
              const parts = r.display_name.split(", ");
              const name = parts.slice(0, 2).join(", ");
              const detail = parts.slice(2).join(", ");
              return (
                <div
                  key={`${r.lat}-${r.lon}`}
                  className={`city-search-item${i === highlighted ? " highlighted" : ""}`}
                  onMouseDown={() => handleSelect(r)}
                  onMouseEnter={() => setHighlighted(i)}
                >
                  <div className="city-search-item-name">{name}</div>
                  {detail && <div className="city-search-item-detail">{detail}</div>}
                </div>
              );
            })}
        </div>
      )}
    </div>
  );
}
