import type {
  ChatResponse,
  ChatStatus,
  DatasetInfo,
  DatasetPage,
  Metadata,
  ScoreRequest,
  ScoreResponse,
} from "./types";

const BASE = "/api";

export async function fetchScore(prefs: ScoreRequest): Promise<ScoreResponse> {
  const res = await fetch(`${BASE}/score`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(prefs),
  });
  if (!res.ok) throw new Error(`Score request failed: ${res.status}`);
  return res.json();
}

export async function fetchMetadata(): Promise<Metadata> {
  const res = await fetch(`${BASE}/metadata`);
  if (!res.ok) throw new Error(`Metadata request failed: ${res.status}`);
  return res.json();
}

export async function fetchDatasets(): Promise<DatasetInfo[]> {
  const res = await fetch(`${BASE}/data/list`);
  if (!res.ok) throw new Error(`Dataset list failed: ${res.status}`);
  return res.json();
}

export async function fetchDatasetPage(
  name: string,
  offset: number,
  limit: number,
): Promise<DatasetPage> {
  const res = await fetch(
    `${BASE}/data/${name}?offset=${offset}&limit=${limit}`,
  );
  if (!res.ok) throw new Error(`Dataset read failed: ${res.status}`);
  return res.json();
}

export async function sendChatMessage(
  message: string,
  history: { role: string; content: string }[],
): Promise<ChatResponse> {
  const res = await fetch(`${BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message, history }),
  });
  if (!res.ok) throw new Error(`Chat request failed: ${res.status}`);
  return res.json();
}

export async function fetchChatStatus(): Promise<ChatStatus> {
  const res = await fetch(`${BASE}/chat/status`);
  if (!res.ok) throw new Error(`Chat status failed: ${res.status}`);
  return res.json();
}
