export interface ScoreRequest {
  monthly_payment: number;
  loan_term_years: number;
  down_payment_pct: number;
  bedbath_bucket: string;
  property_type_pref: string;
  anchor_lat: number;
  anchor_lon: number;
  anchor_state: string;
  max_radius_miles: number;
  pref_mountains: number;
  pref_beach: number;
  pref_lake: number;
  pref_airport: number;
  pref_climate: number;
  pref_terrain: number;
  pref_cost: number;
  preferred_climate: string;
  preferred_terrain: string;
  top_n: number;
}

export interface Enrichment {
  median_household_income?: number;
  poverty_rate?: number;
  median_home_value?: number;
  median_rent?: number;
  pct_owner_occupied?: number;
  commute_work_from_home_pct?: number;
  mean_commute_minutes?: number;
  violent_crime_rate?: number;
  property_crime_rate?: number;
  pct_broadband_100_20?: number;
  num_providers?: number;
  max_download_mbps?: number;
  pm25_mean?: number;
  ozone_mean?: number;
  avg_weekly_wage?: number;
  avg_annual_salary?: number;
  annual_avg_establishments?: number;
  nearest_hospital_name?: string;
  nearest_hospital_miles?: number;
  nearest_hospital_rating?: number;
  hospitals_within_30mi?: number;
  avg_rating_within_30mi?: number;
  total_providers?: number;
  primary_care_count?: number;
  providers_per_1000_pop?: number;
  zip_code?: string;
}

export interface CommunityScore {
  canonical_id: string;
  city_state: string;
  state_name: string;
  latitude: number;
  longitude: number;
  terrain: string;
  climate: string;
  population: number;
  cost_of_living: number;
  median_home_price: number;
  housing_score: number;
  lifestyle_score: number;
  spillover_score: number;
  final_score: number;
  matches_bb: number;
  matches_sfh: number;
  pressure: string;
  spillover_anchor: string;
  spillover_explanation: string;
  dist_to_anchor: number;
  enrichment: Enrichment;
}

export interface ScoreResponse {
  rankings: CommunityScore[];
  total_candidates: number;
  eliminated_count: number;
  max_purchase_price: number;
  affordability_window: [number, number];
}

export interface Metadata {
  states: string[];
  climates: string[];
  terrains: string[];
  bedbath_buckets: string[];
  property_types: string[];
  ranges: {
    monthly_payment: { min: number; max: number; step: number };
    down_payment_pct: { min: number; max: number; step: number };
    max_radius_miles: { min: number; max: number; step: number };
    loan_term_years: number[];
  };
}

export interface DatasetInfo {
  name: string;
  rows: number;
  columns: number;
  column_names: string[];
}

export interface DatasetPage {
  name: string;
  total_rows: number;
  offset: number;
  limit: number;
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  table: Record<string, string>[] | null;
}

export interface ChatResponse {
  role: string;
  content: string;
  table: Record<string, string>[] | null;
}

export interface ChatStatus {
  available: boolean;
}

export const DEFAULT_PREFS: ScoreRequest = {
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
};
