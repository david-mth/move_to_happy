import { useCallback, useEffect, useState } from "react";
import { fetchEDAColumns, fetchEDAData, fetchEDASummary } from "../api";
import type { CommunityIndex, EDADataset, EDAKPIs } from "../types";
import { CommunityOverview } from "./eda/CommunityOverview";
import { CorrelationExplorer } from "./eda/CorrelationExplorer";
import { EconomicTax } from "./eda/EconomicTax";
import { KPIStrip } from "./eda/KPIStrip";
import { LivabilityIndex } from "./eda/LivabilityIndex";
import { SafetyHealth } from "./eda/SafetyHealth";

type Row = Record<string, unknown>;

const TABS = [
  { id: "overview", label: "Community Overview" },
  { id: "livability", label: "Livability Index" },
  { id: "economic", label: "Economic & Tax" },
  { id: "safety", label: "Safety & Health" },
  { id: "correlation", label: "Correlation Explorer" },
] as const;

type TabId = (typeof TABS)[number]["id"];

export function EDAPage() {
  const [tab, setTab] = useState<TabId>("overview");
  const [stateFilter, setStateFilter] = useState("All");
  const [kpis, setKpis] = useState<EDAKPIs | null>(null);
  const [indices, setIndices] = useState<CommunityIndex[]>([]);
  const [datasets, setDatasets] = useState<EDADataset[]>([]);
  const [dataCache, setDataCache] = useState<Record<string, Row[]>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchEDASummary(stateFilter), fetchEDAColumns()])
      .then(([summary, ds]) => {
        setKpis(summary.kpis);
        setIndices(summary.indices);
        setDatasets(ds);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [stateFilter]);

  const loadData = useCallback(
    async (dataset: string, columns: string[]): Promise<Row[]> => {
      const key = `${dataset}:${[...columns].sort().join(",")}:${stateFilter}`;
      if (dataCache[key]) return dataCache[key];
      const result = await fetchEDAData(dataset, columns, stateFilter);
      setDataCache((prev) => ({ ...prev, [key]: result.rows }));
      return result.rows;
    },
    [dataCache, stateFilter],
  );

  if (loading) {
    return (
      <div className="eda-page">
        <div className="eda-loading">Loading analytics...</div>
      </div>
    );
  }

  return (
    <div className="eda-page">
      <div className="eda-header">
        <div>
          <h2>Advanced Analytics</h2>
          <p className="eda-subtitle">
            Cross-dataset insights across {kpis?.total_communities ?? "~1,305"} communities
          </p>
        </div>
        <div className="eda-state-filter">
          {["All", "Georgia", "Alabama", "Florida"].map((s) => (
            <button
              key={s}
              className={`eda-state-btn ${stateFilter === s ? "active" : ""}`}
              onClick={() => {
                setStateFilter(s);
                setDataCache({});
              }}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {kpis && <KPIStrip kpis={kpis} />}

      <div className="eda-tabs">
        {TABS.map((t) => (
          <button
            key={t.id}
            className={`eda-tab ${tab === t.id ? "active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="eda-tab-content">
        {tab === "overview" && (
          <CommunityOverview
            indices={indices}
            stateFilter={stateFilter}
          />
        )}
        {tab === "livability" && (
          <LivabilityIndex
            indices={indices}
            stateFilter={stateFilter}
          />
        )}
        {tab === "economic" && (
          <EconomicTax
            datasets={datasets}
            stateFilter={stateFilter}
            loadData={loadData}
          />
        )}
        {tab === "safety" && (
          <SafetyHealth loadData={loadData} />
        )}
        {tab === "correlation" && (
          <CorrelationExplorer />
        )}
      </div>
    </div>
  );
}
