export type Metrics = {
  businesses: {
    total: number;
    no_website: number;
    scored: number;
    no_website_scored: number;
    no_website_unscored: number;
  };
  domains: Record<string, number>;
  contacts: {
    total: number;
    scored: number;
    unscored: number;
  };
  exports: {
    total: number;
    queued: number;
  };
  business_exports: {
    total: number;
    queued: number;
  };
  verification: Record<string, number>;
  verification_details: {
    ddg_conclusive: number;
    ddg_no_results: number;
    llm_conclusive?: number;
    llm_not_sure?: number;
    searxng_conclusive?: number;
    searxng_no_results?: number;
  };
  confidence_distribution: {
    high: number;
    medium: number;
    low: number;
    unverified: number;
  };
  recent_jobs: {
    job_name: string;
    status: string;
    started_at: string | null;
    finished_at: string | null;
    processed_count: number;
  }[];
};

export type JobRun = {
  id: string;
  job_name: string;
  scope: string | null;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  processed_count: number;
  details?: Record<string, unknown> | null;
  error?: string | null;
};

export type BusinessLead = {
  id: string;
  name: string | null;
  category: string | null;
  address: string | null;
  city: string | null;
  country: string | null;
  lead_score: number | null;
  scored_at: string | null;
  source: string;
  source_id: string;
  emails: string[];
  business_emails: string[];
  free_emails: string[];
  phones: string[];
  domains: string[];
  verified_unhosted_domains: string[];
  unregistered_domains: string[];
  unknown_domains: string[];
  hosted_domains: string[];
  parked_domains: string[];
  domain_status_counts: Record<string, number>;
  exported: boolean;
  verification_count: number;
  verification_sources: string[];
  verification_confidence: string;
};

export type BusinessLeadResponse = {
  total_candidates: number;
  returned: number;
  items: BusinessLead[];
};

export type ExportFile = {
  name: string;
  size: number;
  modified_at: number;
};

export type VerificationSettings = {
  domain_guess_batch: number;
  domain_guess_min_score: number;
  ddg_batch: number;
  ddg_min_score: number;
  llm_batch: number;
  llm_min_score: number;
  google_search_batch: number;
  google_search_min_score: number;
  searxng_batch: number;
  searxng_min_score: number;
  rescore_after_batch: boolean;
  pause_between_batches: number;
  pause_when_idle: number;
};

export type VerificationTotals = {
  domain_guess_processed: number;
  domain_guess_websites: number;
  ddg_processed: number;
  ddg_websites: number;
  llm_processed: number;
  llm_websites: number;
  google_search_processed: number;
  google_search_websites: number;
  searxng_processed: number;
  searxng_websites: number;
  rescored: number;
};

export type VerificationStatus = {
  running: boolean;
  settings: VerificationSettings;
  last_started_at: string | null;
  last_finished_at: string | null;
  last_error: string | null;
  batch_count: number;
  totals: VerificationTotals;
};

export type AutomationStatus = {
  running: boolean;
  busy: boolean;
  settings: {
    interval_seconds: number;
    area: string | null;
    categories: string;
    rdap_statuses: string[];
    daily_target_enabled: boolean;
    daily_target_count: number;
    daily_target_min_score: number;
    daily_target_platform_prefix: string;
    daily_target_require_contact: boolean;
    daily_target_require_domain_qualification: boolean;
    daily_target_require_unhosted_domain: boolean;
    daily_target_allow_recycle: boolean;
    [key: string]: unknown;
  };
  last_run_started_at: string | null;
  last_run_finished_at: string | null;
  last_error: string | null;
  last_result: Record<string, unknown> | null;
  run_count: number;
  verification?: VerificationStatus;
};
