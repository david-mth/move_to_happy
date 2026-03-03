import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchDatasetPage, fetchDatasets } from "../api";
import type { DatasetInfo, DatasetPage } from "../types";

const PAGE_SIZE = 50;

export function DataExplorer() {
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [active, setActive] = useState<string>("");
  const [page, setPage] = useState<DatasetPage | null>(null);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState("");
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  useEffect(() => {
    fetchDatasets().then((ds) => {
      setDatasets(ds);
      if (ds.length > 0) setActive(ds[0].name);
    });
  }, []);

  useEffect(() => {
    if (!active) return;
    setLoading(true);
    setSearch("");
    setSortCol(null);
    fetchDatasetPage(active, 0, 10_000)
      .then((p) => {
        setPage(p);
        setOffset(0);
      })
      .finally(() => setLoading(false));
  }, [active]);

  const handleSort = useCallback(
    (col: string) => {
      if (sortCol === col) {
        setSortAsc((prev) => !prev);
      } else {
        setSortCol(col);
        setSortAsc(true);
      }
    },
    [sortCol],
  );

  const filteredAndSorted = useMemo(() => {
    if (!page) return [];
    let rows = page.rows;

    if (search.trim()) {
      const q = search.toLowerCase();
      rows = rows.filter((r) =>
        Object.values(r).some((v) =>
          String(v ?? "")
            .toLowerCase()
            .includes(q),
        ),
      );
    }

    if (sortCol) {
      rows = [...rows].sort((a, b) => {
        const va = a[sortCol] ?? "";
        const vb = b[sortCol] ?? "";
        const na = Number(va);
        const nb = Number(vb);
        if (!isNaN(na) && !isNaN(nb)) return sortAsc ? na - nb : nb - na;
        return sortAsc
          ? String(va).localeCompare(String(vb))
          : String(vb).localeCompare(String(va));
      });
    }

    return rows;
  }, [page, search, sortCol, sortAsc]);

  const pageRows = useMemo(
    () => filteredAndSorted.slice(offset, offset + PAGE_SIZE),
    [filteredAndSorted, offset],
  );

  const totalFiltered = filteredAndSorted.length;
  const totalPages = Math.ceil(totalFiltered / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  const activeInfo = datasets.find((d) => d.name === active);

  const formatCell = (val: unknown): string => {
    if (val === null || val === undefined) return "—";
    if (typeof val === "number") {
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toLocaleString(undefined, { maximumFractionDigits: 4 });
    }
    return String(val);
  };

  return (
    <div className="data-explorer">
      <div className="de-sidebar">
        <h3>Datasets</h3>
        <div className="de-dataset-list">
          {datasets.map((ds) => (
            <button
              key={ds.name}
              className={`de-dataset-item ${ds.name === active ? "active" : ""}`}
              onClick={() => setActive(ds.name)}
            >
              <span className="de-ds-name">
                {ds.name.replace("tier1/", "").replace(".csv", "")}
              </span>
              <span className="de-ds-meta">
                {ds.rows.toLocaleString()} rows &middot; {ds.columns} cols
              </span>
            </button>
          ))}
        </div>
      </div>

      <div className="de-main">
        <div className="de-toolbar">
          <div className="de-title">
            <h2>{activeInfo?.name.replace("tier1/", "").replace(".csv", "")}</h2>
            {activeInfo && (
              <span className="de-row-count">
                {activeInfo.rows.toLocaleString()} rows &middot;{" "}
                {activeInfo.columns} columns
              </span>
            )}
          </div>
          <input
            type="text"
            className="de-search"
            placeholder="Search across all columns..."
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setOffset(0);
            }}
          />
        </div>

        {loading ? (
          <div className="de-loading">Loading dataset...</div>
        ) : page ? (
          <>
            <div className="de-table-wrap">
              <table className="de-table">
                <thead>
                  <tr>
                    <th className="de-row-num">#</th>
                    {page.columns.map((col) => (
                      <th
                        key={col}
                        onClick={() => handleSort(col)}
                        className={sortCol === col ? "sorted" : ""}
                      >
                        <span className="de-th-content">
                          {col}
                          {sortCol === col && (
                            <span className="de-sort-arrow">
                              {sortAsc ? " ▲" : " ▼"}
                            </span>
                          )}
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row, i) => (
                    <tr key={offset + i}>
                      <td className="de-row-num">{offset + i + 1}</td>
                      {page.columns.map((col) => (
                        <td key={col}>{formatCell(row[col])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="de-pagination">
              <button
                disabled={offset === 0}
                onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
              >
                ← Prev
              </button>
              <span>
                Page {currentPage} of {totalPages}
                {search && (
                  <span className="de-filter-count">
                    {" "}
                    ({totalFiltered.toLocaleString()} matching)
                  </span>
                )}
              </span>
              <button
                disabled={offset + PAGE_SIZE >= totalFiltered}
                onClick={() => setOffset(offset + PAGE_SIZE)}
              >
                Next →
              </button>
            </div>
          </>
        ) : (
          <div className="de-loading">Select a dataset to explore</div>
        )}
      </div>
    </div>
  );
}
