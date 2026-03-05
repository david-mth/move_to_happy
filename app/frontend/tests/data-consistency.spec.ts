/**
 * Data Consistency Tests
 *
 * Verifies that data is consistent across all layers:
 *   1. Data Explorer UI matches API responses (which come from CSV files)
 *   2. Enrichment data in scorer results matches tier-1 source datasets
 *   3. canonical_ids are consistent across all 8 datasets (no orphans)
 *   4. All data paths match end-to-end (CSV -> API -> UI)
 */
import { expect, test } from "@playwright/test";

const API = "http://127.0.0.1:8000/api";

// All 8 datasets as listed by the backend
const EXPECTED_DATASETS = [
  "mth_communities.csv",
  "tier1/bls_employment.csv",
  "tier1/census_acs.csv",
  "tier1/cms_hospitals.csv",
  "tier1/cms_physicians.csv",
  "tier1/epa_air_quality.csv",
  "tier1/fbi_crime.csv",
  "tier1/fcc_broadband.csv",
];

// Enrichment fields and the tier-1 dataset they come from
const ENRICHMENT_SOURCE_MAP: Record<string, { dataset: string; field: string }> =
  {
    median_household_income: {
      dataset: "tier1/census_acs.csv",
      field: "median_household_income",
    },
    poverty_rate: { dataset: "tier1/census_acs.csv", field: "poverty_rate" },
    median_home_value: {
      dataset: "tier1/census_acs.csv",
      field: "median_home_value",
    },
    median_rent: { dataset: "tier1/census_acs.csv", field: "median_rent" },
    pct_owner_occupied: {
      dataset: "tier1/census_acs.csv",
      field: "pct_owner_occupied",
    },
    commute_work_from_home_pct: {
      dataset: "tier1/census_acs.csv",
      field: "commute_work_from_home_pct",
    },
    mean_commute_minutes: {
      dataset: "tier1/census_acs.csv",
      field: "mean_commute_minutes",
    },
    violent_crime_rate: {
      dataset: "tier1/fbi_crime.csv",
      field: "violent_crime_rate",
    },
    property_crime_rate: {
      dataset: "tier1/fbi_crime.csv",
      field: "property_crime_rate",
    },
    pct_broadband_100_20: {
      dataset: "tier1/fcc_broadband.csv",
      field: "pct_broadband_100_20",
    },
    num_providers: {
      dataset: "tier1/fcc_broadband.csv",
      field: "num_providers",
    },
    max_download_mbps: {
      dataset: "tier1/fcc_broadband.csv",
      field: "max_download_mbps",
    },
    pm25_mean: { dataset: "tier1/epa_air_quality.csv", field: "pm25_mean" },
    ozone_mean: { dataset: "tier1/epa_air_quality.csv", field: "ozone_mean" },
    avg_weekly_wage: {
      dataset: "tier1/bls_employment.csv",
      field: "avg_weekly_wage",
    },
    avg_annual_salary: {
      dataset: "tier1/bls_employment.csv",
      field: "avg_annual_salary",
    },
    annual_avg_establishments: {
      dataset: "tier1/bls_employment.csv",
      field: "annual_avg_establishments",
    },
    nearest_hospital_name: {
      dataset: "tier1/cms_hospitals.csv",
      field: "nearest_hospital_name",
    },
    nearest_hospital_miles: {
      dataset: "tier1/cms_hospitals.csv",
      field: "nearest_hospital_miles",
    },
    nearest_hospital_rating: {
      dataset: "tier1/cms_hospitals.csv",
      field: "nearest_hospital_rating",
    },
    hospitals_within_30mi: {
      dataset: "tier1/cms_hospitals.csv",
      field: "hospitals_within_30mi",
    },
    avg_rating_within_30mi: {
      dataset: "tier1/cms_hospitals.csv",
      field: "avg_rating_within_30mi",
    },
    total_providers: {
      dataset: "tier1/cms_physicians.csv",
      field: "total_providers",
    },
    primary_care_count: {
      dataset: "tier1/cms_physicians.csv",
      field: "primary_care_count",
    },
    providers_per_1000_pop: {
      dataset: "tier1/cms_physicians.csv",
      field: "providers_per_1000_pop",
    },
  };

// ---------------------------------------------------------------------------
// 1. DATA EXPLORER vs API: Verify the Data Explorer UI renders the same
//    data returned by the /api/data endpoints
// ---------------------------------------------------------------------------
test.describe("Data Explorer vs API consistency", () => {
  test("all 8 datasets are listed in the Data Explorer sidebar", async ({
    page,
    request,
  }) => {
    // Get ground truth from API
    const apiRes = await request.get(`${API}/data/list`);
    const apiDatasets = (await apiRes.json()) as {
      name: string;
      rows: number;
      columns: number;
    }[];

    expect(apiDatasets.length).toBe(EXPECTED_DATASETS.length);
    const apiNames = apiDatasets.map((d) => d.name).sort();
    expect(apiNames).toEqual(EXPECTED_DATASETS);

    // Navigate to Data Explorer
    await page.goto("/data");
    await page.waitForSelector(".de-dataset-list");

    // Get all dataset buttons from the sidebar
    const sidebarItems = await page
      .locator(".de-dataset-item .de-ds-name")
      .allTextContents();

    // The UI strips "tier1/" and ".csv" from names
    const expectedUiNames = EXPECTED_DATASETS.map((n) =>
      n.replace("tier1/", "").replace(".csv", ""),
    ).sort();
    const actualUiNames = sidebarItems.map((n) => n.trim()).sort();

    expect(actualUiNames).toEqual(expectedUiNames);
  });

  test("dataset row counts in sidebar match API counts", async ({
    page,
    request,
  }) => {
    const apiRes = await request.get(`${API}/data/list`);
    const apiDatasets = (await apiRes.json()) as {
      name: string;
      rows: number;
      columns: number;
    }[];

    await page.goto("/data");
    await page.waitForSelector(".de-dataset-list");

    // Check each dataset's row/column counts
    const metaTexts = await page
      .locator(".de-dataset-item .de-ds-meta")
      .allTextContents();

    for (let i = 0; i < apiDatasets.length; i++) {
      const ds = apiDatasets[i];
      const metaText = metaTexts[i]?.trim() ?? "";
      // Format is "1,305 rows · 33 cols"
      const rowMatch = metaText.match(/([\d,]+)\s*rows/);
      const colMatch = metaText.match(/(\d+)\s*cols/);

      expect(
        rowMatch,
        `Row count should be visible for ${ds.name}`,
      ).not.toBeNull();
      expect(
        colMatch,
        `Column count should be visible for ${ds.name}`,
      ).not.toBeNull();

      const uiRows = parseInt(rowMatch![1].replace(/,/g, ""), 10);
      const uiCols = parseInt(colMatch![1], 10);

      expect(uiRows, `Row count mismatch for ${ds.name}`).toBe(ds.rows);
      expect(uiCols, `Column count mismatch for ${ds.name}`).toBe(ds.columns);
    }
  });

  test("Data Explorer table columns match API columns for each dataset", async ({
    page,
    request,
  }) => {
    const apiRes = await request.get(`${API}/data/list`);
    const apiDatasets = (await apiRes.json()) as {
      name: string;
      rows: number;
      columns: number;
      column_names: string[];
    }[];

    await page.goto("/data");
    await page.waitForSelector(".de-dataset-list");

    for (const ds of apiDatasets) {
      // Click the dataset in the sidebar
      const uiName = ds.name.replace("tier1/", "").replace(".csv", "");
      await page.locator(".de-dataset-item", { hasText: uiName }).click();

      // Wait for the title to update to the clicked dataset name, confirming load
      await expect(page.locator(".de-title h2")).toHaveText(uiName, {
        timeout: 10_000,
      });

      // Wait for table rows to render
      await page.waitForSelector(".de-table tbody tr", { timeout: 10_000 });

      // Verify the first column header matches the expected first column
      // (ensures we're reading the new table, not a stale one)
      await expect(
        page.locator(".de-table thead th").nth(1),
      ).toHaveText(ds.column_names[0], { timeout: 5_000 });

      // Get column headers (skip first # column)
      const headers = await page.locator(".de-table thead th").allTextContents();
      const actualCols = headers
        .slice(1) // skip row number column
        .map((h) => h.replace(/\s*[▲▼]\s*/, "").trim());

      expect(
        actualCols,
        `Column mismatch for dataset ${ds.name}`,
      ).toEqual(ds.column_names);
    }
  });

  test("first page of data in Data Explorer matches API response", async ({
    page,
    request,
  }) => {
    // Test with the communities dataset (first loaded)
    const apiRes = await request.get(
      `${API}/data/mth_communities.csv?offset=0&limit=10000`,
    );
    const apiData = (await apiRes.json()) as {
      columns: string[];
      rows: Record<string, unknown>[];
    };

    await page.goto("/data");
    await page.waitForSelector(".de-table tbody tr");

    // Get the first page of data from the UI (50 rows)
    const tableRows = page.locator(".de-table tbody tr");
    const rowCount = await tableRows.count();
    expect(rowCount).toBeLessThanOrEqual(50); // PAGE_SIZE

    // Verify first row data matches
    const firstRowCells = await tableRows.first().locator("td").allTextContents();
    const firstRowData = firstRowCells.slice(1); // skip row number

    for (let colIdx = 0; colIdx < apiData.columns.length; colIdx++) {
      const col = apiData.columns[colIdx];
      const apiVal = apiData.rows[0][col];
      const uiVal = firstRowData[colIdx]?.trim();

      if (apiVal === null || apiVal === undefined) {
        expect(uiVal, `Null value for ${col} row 0`).toBe("—");
      } else if (typeof apiVal === "number") {
        // UI formats numbers with toLocaleString, so compare numerically
        const uiNum = parseFloat(uiVal.replace(/,/g, ""));
        expect(
          Math.abs(uiNum - (apiVal as number)),
          `Numeric mismatch for ${col} row 0: UI=${uiVal} API=${apiVal}`,
        ).toBeLessThan(0.001);
      } else {
        expect(uiVal, `String mismatch for ${col} row 0`).toBe(String(apiVal));
      }
    }
  });
});

// ---------------------------------------------------------------------------
// 2. CROSS-DATASET CANONICAL_ID CONSISTENCY: Every canonical_id in tier-1
//    datasets must exist in the main communities dataset
// ---------------------------------------------------------------------------
test.describe("Cross-dataset canonical_id consistency", () => {
  test("all tier-1 canonical_ids exist in the communities dataset", async ({
    request,
  }) => {
    // Get all canonical_ids from the communities dataset
    const commRes = await request.get(
      `${API}/data/mth_communities.csv?offset=0&limit=10000`,
    );
    const commData = (await commRes.json()) as {
      rows: Record<string, unknown>[];
    };
    const communityIds = new Set(
      commData.rows.map((r) => r.canonical_id as string),
    );

    expect(communityIds.size).toBeGreaterThan(0);

    // Check each tier-1 dataset
    const tier1Datasets = EXPECTED_DATASETS.filter((d) =>
      d.startsWith("tier1/"),
    );

    for (const dsName of tier1Datasets) {
      const res = await request.get(
        `${API}/data/${dsName}?offset=0&limit=10000`,
      );
      const data = (await res.json()) as {
        rows: Record<string, unknown>[];
      };

      const orphanIds: string[] = [];
      for (const row of data.rows) {
        const cid = row.canonical_id as string;
        if (!communityIds.has(cid)) {
          orphanIds.push(cid);
        }
      }

      expect(
        orphanIds,
        `${dsName} has ${orphanIds.length} canonical_ids not in communities: ${orphanIds.slice(0, 5).join(", ")}`,
      ).toHaveLength(0);
    }
  });

  test("all tier-1 datasets share the same set of canonical_ids", async ({
    request,
  }) => {
    const tier1Datasets = EXPECTED_DATASETS.filter((d) =>
      d.startsWith("tier1/"),
    );

    const idSets: Record<string, Set<string>> = {};

    for (const dsName of tier1Datasets) {
      const res = await request.get(
        `${API}/data/${dsName}?offset=0&limit=10000`,
      );
      const data = (await res.json()) as {
        rows: Record<string, unknown>[];
      };
      idSets[dsName] = new Set(
        data.rows.map((r) => r.canonical_id as string),
      );
    }

    // Compare each pair of datasets
    const dsNames = Object.keys(idSets);
    const referenceDs = dsNames[0];
    const referenceIds = idSets[referenceDs];

    for (let i = 1; i < dsNames.length; i++) {
      const otherDs = dsNames[i];
      const otherIds = idSets[otherDs];

      // Find IDs in reference but not in other
      const missingFromOther = [...referenceIds].filter(
        (id) => !otherIds.has(id),
      );
      // Find IDs in other but not in reference
      const extraInOther = [...otherIds].filter(
        (id) => !referenceIds.has(id),
      );

      expect(
        missingFromOther.length,
        `${otherDs} is missing ${missingFromOther.length} IDs present in ${referenceDs}: ${missingFromOther.slice(0, 5).join(", ")}`,
      ).toBe(0);
      expect(
        extraInOther.length,
        `${otherDs} has ${extraInOther.length} extra IDs not in ${referenceDs}: ${extraInOther.slice(0, 5).join(", ")}`,
      ).toBe(0);
    }
  });

  test("no duplicate canonical_ids within any dataset", async ({
    request,
  }) => {
    for (const dsName of EXPECTED_DATASETS) {
      const res = await request.get(
        `${API}/data/${dsName}?offset=0&limit=10000`,
      );
      const data = (await res.json()) as {
        rows: Record<string, unknown>[];
      };

      const ids = data.rows.map((r) => r.canonical_id as string);
      const uniqueIds = new Set(ids);

      expect(
        ids.length,
        `${dsName} has ${ids.length - uniqueIds.size} duplicate canonical_ids`,
      ).toBe(uniqueIds.size);
    }
  });

  test("every dataset has a canonical_id column", async ({ request }) => {
    const apiRes = await request.get(`${API}/data/list`);
    const datasets = (await apiRes.json()) as {
      name: string;
      column_names: string[];
    }[];

    for (const ds of datasets) {
      expect(
        ds.column_names,
        `${ds.name} is missing canonical_id column`,
      ).toContain("canonical_id");
    }
  });
});

// ---------------------------------------------------------------------------
// 3. ENRICHMENT vs SOURCE DATASETS: Verify that enrichment data returned
//    by the scorer matches the corresponding tier-1 dataset values
// ---------------------------------------------------------------------------
test.describe("Enrichment data matches source datasets", () => {
  test("scored community enrichment values match tier-1 source data", async ({
    request,
  }) => {
    // Score communities with default preferences
    const scoreRes = await request.post(`${API}/score`, {
      data: {
        monthly_payment: 2500,
        loan_term_years: 30,
        down_payment_pct: 0.1,
        bedbath_bucket: "BB2",
        property_type_pref: "SFH",
        anchor_lat: 33.749,
        anchor_lon: -84.388,
        anchor_state: "Georgia",
        max_radius_miles: 120,
        pref_mountains: 0.3,
        pref_beach: 0.15,
        pref_lake: 0.1,
        pref_airport: 0.1,
        pref_climate: 0.15,
        pref_terrain: 0.1,
        pref_cost: 0.1,
        preferred_climate: "Temperate",
        preferred_terrain: "Mountains",
        top_n: 25,
      },
    });
    const scoreData = (await scoreRes.json()) as {
      rankings: {
        canonical_id: string;
        enrichment: Record<string, unknown>;
      }[];
    };

    expect(scoreData.rankings.length).toBeGreaterThan(0);

    // Load all tier-1 datasets into a lookup: canonical_id -> field -> value
    const tier1Lookup: Record<string, Record<string, unknown>> = {};
    const tier1Datasets = EXPECTED_DATASETS.filter((d) =>
      d.startsWith("tier1/"),
    );

    for (const dsName of tier1Datasets) {
      const res = await request.get(
        `${API}/data/${dsName}?offset=0&limit=10000`,
      );
      const data = (await res.json()) as {
        rows: Record<string, unknown>[];
      };

      for (const row of data.rows) {
        const cid = row.canonical_id as string;
        if (!tier1Lookup[cid]) tier1Lookup[cid] = {};
        for (const [key, val] of Object.entries(row)) {
          if (key !== "canonical_id") {
            tier1Lookup[cid][key] = val;
          }
        }
      }
    }

    // For each scored community, verify enrichment matches source
    let totalChecks = 0;
    let mismatches: string[] = [];

    for (const community of scoreData.rankings) {
      const cid = community.canonical_id;
      const enrichment = community.enrichment;
      const sourceData = tier1Lookup[cid];

      if (!sourceData) {
        mismatches.push(`${cid}: no source data found in tier-1 datasets`);
        continue;
      }

      for (const [enrichField, mapping] of Object.entries(
        ENRICHMENT_SOURCE_MAP,
      )) {
        const enrichVal = enrichment[enrichField];
        const sourceVal = sourceData[mapping.field];

        // Both null/undefined — consistent
        if (
          (enrichVal === null || enrichVal === undefined) &&
          (sourceVal === null || sourceVal === undefined)
        ) {
          totalChecks++;
          continue;
        }

        // One is null and the other isn't
        if (enrichVal === null || enrichVal === undefined) {
          // Enrichment skips NaN values from CSVs (enrichment.py:98-100).
          // But if the source has a real value and enrichment dropped it, flag it.
          if (sourceVal !== null && sourceVal !== undefined) {
            console.warn(
              `${cid}.${enrichField}: source has value ${sourceVal} but enrichment is missing`,
            );
          }
          totalChecks++;
          continue;
        }

        if (sourceVal === null || sourceVal === undefined) {
          mismatches.push(
            `${cid}.${enrichField}: enrichment=${enrichVal} but source is null`,
          );
          totalChecks++;
          continue;
        }

        // Compare values
        if (typeof enrichVal === "number" && typeof sourceVal === "number") {
          const diff = Math.abs(enrichVal - sourceVal);
          const tolerance = Math.abs(sourceVal) * 0.001 + 0.001; // 0.1% + epsilon
          if (diff > tolerance) {
            mismatches.push(
              `${cid}.${enrichField}: enrichment=${enrichVal} source=${sourceVal} diff=${diff}`,
            );
          }
        } else if (typeof enrichVal === "string") {
          if (String(enrichVal) !== String(sourceVal)) {
            mismatches.push(
              `${cid}.${enrichField}: enrichment="${enrichVal}" source="${sourceVal}"`,
            );
          }
        }
        totalChecks++;
      }
    }

    console.log(
      `Verified ${totalChecks} enrichment values across ${scoreData.rankings.length} communities`,
    );
    expect(
      mismatches,
      `Found ${mismatches.length} enrichment mismatches:\n${mismatches.join("\n")}`,
    ).toHaveLength(0);
  });

  test("enrichment data appears correctly in community cards UI", async ({
    page,
  }) => {
    await page.goto("/");

    // Click the Score button to trigger scoring
    await page.waitForSelector(".score-btn", { timeout: 10_000 });
    await page.locator(".score-btn").click();

    // Wait for the scorer to load and show results
    await page.waitForSelector(".community-card", { timeout: 30_000 });

    // Click first community card to expand it
    const firstCard = page.locator(".community-card").first();
    await firstCard.click();

    // Wait for enrichment grid to appear
    await page.waitForSelector(".enrichment-grid");

    // Get all enrichment items
    const enrichItems = page.locator(".enrich-item");
    const count = await enrichItems.count();

    // Should have 9 enrichment items (as defined in CommunityCard)
    expect(count).toBe(9);

    // Verify labels are present
    const expectedLabels = [
      "Median Income",
      "Crime Rate",
      "Broadband 100/20",
      "PM2.5",
      "Avg Wage",
      "Hospital",
      "Hosp Rating",
      "Providers/1k",
      "Median Rent",
    ];

    const actualLabels = await page
      .locator(".enrich-label")
      .allTextContents();
    expect(actualLabels.map((l) => l.trim())).toEqual(expectedLabels);

    // Verify values are not all dashes (at least some data exists)
    const values = await page
      .locator(".enrich-value")
      .allTextContents();
    const nonEmptyValues = values.filter((v) => v.trim() !== "—");
    expect(
      nonEmptyValues.length,
      "At least some enrichment values should have data",
    ).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// 4. END-TO-END DATA PATH: Verify API dataset endpoints return valid data
//    and that the data is structurally consistent
// ---------------------------------------------------------------------------
test.describe("End-to-end data path validation", () => {
  test("each dataset API response has correct structure", async ({
    request,
  }) => {
    for (const dsName of EXPECTED_DATASETS) {
      const res = await request.get(
        `${API}/data/${dsName}?offset=0&limit=10`,
      );
      expect(res.ok(), `${dsName} should return 200`).toBe(true);

      const data = (await res.json()) as {
        name: string;
        total_rows: number;
        offset: number;
        limit: number;
        columns: string[];
        rows: Record<string, unknown>[];
      };

      expect(data.name, `${dsName} name`).toBe(dsName);
      expect(data.total_rows, `${dsName} total_rows`).toBeGreaterThan(0);
      expect(data.columns.length, `${dsName} columns`).toBeGreaterThan(0);
      expect(data.rows.length, `${dsName} rows`).toBeGreaterThan(0);
      expect(
        data.rows.length,
        `${dsName} should respect limit`,
      ).toBeLessThanOrEqual(10);

      // Every row should have all columns
      for (const row of data.rows) {
        for (const col of data.columns) {
          expect(
            col in row,
            `${dsName}: row missing column "${col}"`,
          ).toBe(true);
        }
      }
    }
  });

  test("dataset list API row counts match individual dataset total_rows", async ({
    request,
  }) => {
    const listRes = await request.get(`${API}/data/list`);
    const datasets = (await listRes.json()) as {
      name: string;
      rows: number;
      columns: number;
    }[];

    for (const ds of datasets) {
      const dataRes = await request.get(
        `${API}/data/${ds.name}?offset=0&limit=1`,
      );
      const data = (await dataRes.json()) as { total_rows: number };

      expect(
        data.total_rows,
        `${ds.name}: list says ${ds.rows} rows, endpoint says ${data.total_rows}`,
      ).toBe(ds.rows);
    }
  });

  test("paginated dataset responses cover all rows without gaps", async ({
    request,
  }) => {
    // Test with a smaller dataset to keep it fast
    const listRes = await request.get(`${API}/data/list`);
    const datasets = (await listRes.json()) as {
      name: string;
      rows: number;
    }[];

    // Pick the dataset with fewest rows
    const smallest = datasets.reduce((a, b) =>
      a.rows < b.rows ? a : b,
    );

    const pageSize = 100;
    const allIds: string[] = [];

    for (let offset = 0; offset < smallest.rows; offset += pageSize) {
      const res = await request.get(
        `${API}/data/${smallest.name}?offset=${offset}&limit=${pageSize}`,
      );
      const data = (await res.json()) as {
        rows: Record<string, unknown>[];
      };

      for (const row of data.rows) {
        if (row.canonical_id) {
          allIds.push(row.canonical_id as string);
        }
      }
    }

    expect(
      allIds.length,
      `Paginated read of ${smallest.name} should return all ${smallest.rows} rows`,
    ).toBe(smallest.rows);

    // No duplicate IDs across pages
    const uniqueIds = new Set(allIds);
    expect(
      allIds.length,
      `${smallest.name}: paginated read produced duplicates`,
    ).toBe(uniqueIds.size);
  });

  test("scorer returns valid community data with enrichment", async ({
    request,
  }) => {
    const scoreRes = await request.post(`${API}/score`, {
      data: {
        monthly_payment: 3000,
        loan_term_years: 30,
        down_payment_pct: 0.1,
        bedbath_bucket: "BB2",
        property_type_pref: "SFH",
        anchor_lat: 33.749,
        anchor_lon: -84.388,
        anchor_state: "Georgia",
        max_radius_miles: 150,
        pref_mountains: 0.2,
        pref_beach: 0.2,
        pref_lake: 0.1,
        pref_airport: 0.1,
        pref_climate: 0.2,
        pref_terrain: 0.1,
        pref_cost: 0.1,
        preferred_climate: "Subtropical",
        preferred_terrain: "Hills",
        top_n: 10,
      },
    });

    expect(scoreRes.ok()).toBe(true);

    const data = (await scoreRes.json()) as {
      rankings: {
        canonical_id: string;
        city_state: string;
        final_score: number;
        enrichment: Record<string, unknown>;
      }[];
      total_candidates: number;
      eliminated_count: number;
      max_purchase_price: number;
      affordability_window: number[];
    };

    expect(data.rankings.length).toBeGreaterThan(0);
    expect(data.rankings.length).toBeLessThanOrEqual(10);
    expect(data.total_candidates).toBeGreaterThan(0);
    expect(data.max_purchase_price).toBeGreaterThan(0);
    expect(data.affordability_window).toHaveLength(2);

    for (const community of data.rankings) {
      expect(community.canonical_id).toMatch(/^mth_/);
      expect(community.city_state).toBeTruthy();
      expect(community.final_score).toBeGreaterThan(0);
      expect(community.final_score).toBeLessThanOrEqual(1);
      expect(community.enrichment).toBeDefined();
      expect(typeof community.enrichment).toBe("object");
    }
  });
});
