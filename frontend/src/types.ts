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
  recent_jobs: JobRun[];
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
};
