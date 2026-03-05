import { useCallback, useEffect, useState } from "react";
import { fetchEDAColumns, fetchEDAData } from "../api";
import type { EDADataset } from "../types";
import { DistributionChart } from "./eda/DistributionChart";
import { GeoHeatmap } from "./eda/GeoHeatmap";
import { RankingTable } from "./eda/RankingTable";
import { ScatterPlot } from "./eda/ScatterPlot";
import { StateComparison } from "./eda/StateComparison";

type Row = Record<string, unknown>;

interface PanelProps {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}

function Panel({ title, defaultOpen = false, children }: PanelProps) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className={`eda-panel ${open ? "open" : ""}`}>
      <button className="eda-panel-header" onClick={() => setOpen(!open)}>
        <span className="eda-panel-title">{title}</span>
        <span className="eda-panel-chevron">{open ? "▾" : "▸"}</span>
      </button>
      {open && <div className="eda-panel-body">{children}</div>}
    </div>
  );
}

export function EDAPage() {
  const [datasets, setDatasets] = useState<EDADataset[]>([]);
  const [stateFilter, setStateFilter] = useState<string>("All");
  const [dataCache, setDataCache] = useState<Record<string, Row[]>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchEDAColumns()
      .then(setDatasets)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const loadData = useCallback(
    async (dataset: string, columns: string[]): Promise<Row[]> => {
      const key = `${dataset}:${columns.sort().join(",")}`;
      if (dataCache[key]) return dataCache[key];
      const result = await fetchEDAData(dataset, columns);
      setDataCache((prev) => ({ ...prev, [key]: result.rows }));
      return result.rows;
    },
    [dataCache],
  );

  if (loading) {
    return (
      <div className="eda-page">
        <div className="eda-loading">Loading datasets...</div>
      </div>
    );
  }

  return (
    <div className="eda-page">
      <div className="eda-header">
        <div>
          <h2>Exploratory Data Analysis</h2>
          <p className="eda-subtitle">
            {datasets.length} datasets across ~1,305 communities
          </p>
        </div>
        <div className="eda-state-filter">
          {["All", "Georgia", "Alabama", "Florida"].map((s) => (
            <button
              key={s}
              className={`eda-state-btn ${stateFilter === s ? "active" : ""}`}
              onClick={() => setStateFilter(s)}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      <div className="eda-panels">
        <Panel title="Distribution Explorer" defaultOpen>
          <DistributionChart
            datasets={datasets}
            stateFilter={stateFilter}
            loadData={loadData}
          />
        </Panel>

        <Panel title="Scatter Plot / Correlation">
          <ScatterPlot
            datasets={datasets}
            stateFilter={stateFilter}
            loadData={loadData}
          />
        </Panel>

        <Panel title="State Comparison">
          <StateComparison
            datasets={datasets}
            loadData={loadData}
          />
        </Panel>

        <Panel title="Geographic Heatmap">
          <GeoHeatmap
            datasets={datasets}
            stateFilter={stateFilter}
            loadData={loadData}
          />
        </Panel>

        <Panel title="Top / Bottom Rankings">
          <RankingTable
            datasets={datasets}
            stateFilter={stateFilter}
            loadData={loadData}
          />
        </Panel>
      </div>
    </div>
  );
}
