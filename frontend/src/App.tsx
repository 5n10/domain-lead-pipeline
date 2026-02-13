import { FormEvent, useEffect, useMemo, useState } from "react";

import { api } from "./api";
import type { AutomationStatus, BusinessLead, BusinessLeadResponse, ExportFile, JobRun, Metrics } from "./types";

type LeadFilters = {
  minScore: string;
  category: string;
  city: string;
  requireContact: boolean;
  requireUnhostedDomain: boolean;
  requireDomainQualification: boolean;
  requireNoWebsite: boolean;
  onlyUnexported: boolean;
  limit: string;
};

const defaultFilters: LeadFilters = {
  minScore: "",
  category: "all",
  city: "",
  requireContact: false,
  requireUnhostedDomain: false,
  requireDomainQualification: false,
  requireNoWebsite: true,
  onlyUnexported: false,
  limit: "200"
};

function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  const date = new Date(value);
  return date.toLocaleString();
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function leadRow(lead: BusinessLead): JSX.Element {
  const candidateDomains =
    lead.verified_unhosted_domains.join(", ") || lead.unregistered_domains.join(", ") || lead.domains.join(", ");
  return (
    <tr key={lead.id}>
      <td>{lead.name || "-"}</td>
      <td>{lead.category || "-"}</td>
      <td>{lead.city || "-"}</td>
      <td>{lead.lead_score ?? "-"}</td>
      <td>{lead.business_emails.join(", ") || lead.emails.join(", ") || "-"}</td>
      <td>{lead.phones.join(", ") || "-"}</td>
      <td>{candidateDomains || "-"}</td>
      <td>{lead.exported ? "yes" : "no"}</td>
    </tr>
  );
}

export default function App() {
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [automation, setAutomation] = useState<AutomationStatus | null>(null);
  const [jobs, setJobs] = useState<JobRun[]>([]);
  const [leads, setLeads] = useState<BusinessLeadResponse | null>(null);
  const [categories, setCategories] = useState<string[]>([]);
  const [cities, setCities] = useState<string[]>([]);
  const [exportFiles, setExportFiles] = useState<ExportFile[]>([]);
  const [filters, setFilters] = useState<LeadFilters>(defaultFilters);

  const [statusMessage, setStatusMessage] = useState<string>("Ready");
  const [loading, setLoading] = useState<boolean>(false);
  const [actionLoading, setActionLoading] = useState<boolean>(false);

  const [pipelineArea, setPipelineArea] = useState<string>("");
  const [pipelineSyncLimit, setPipelineSyncLimit] = useState<string>("500");
  const [pipelineRdapLimit, setPipelineRdapLimit] = useState<string>("100");
  const [pipelineBusinessScoreLimit, setPipelineBusinessScoreLimit] = useState<string>("2000");

  const [scoreLimit, setScoreLimit] = useState<string>("3000");
  const [scoreForceRescore, setScoreForceRescore] = useState<boolean>(false);

  const [exportPlatform, setExportPlatform] = useState<string>("csv_business");
  const [exportMinScore, setExportMinScore] = useState<string>("40");
  const [exportRequireContact, setExportRequireContact] = useState<boolean>(true);
  const [exportRequireUnhosted, setExportRequireUnhosted] = useState<boolean>(false);
  const [exportRequireDomainQualification, setExportRequireDomainQualification] = useState<boolean>(false);
  const [autoArea, setAutoArea] = useState<string>("");
  const [autoIntervalSeconds, setAutoIntervalSeconds] = useState<string>("900");
  const [autoSyncLimit, setAutoSyncLimit] = useState<string>("100");
  const [autoRdapLimit, setAutoRdapLimit] = useState<string>("5");
  const [autoBusinessScoreLimit, setAutoBusinessScoreLimit] = useState<string>("500");
  const [dailyTargetEnabled, setDailyTargetEnabled] = useState<boolean>(true);
  const [dailyTargetAllowRecycle, setDailyTargetAllowRecycle] = useState<boolean>(true);
  const [dailyTargetCount, setDailyTargetCount] = useState<string>("100");
  const [dailyTargetMinScore, setDailyTargetMinScore] = useState<string>("40");

  const domainStatusBadges = useMemo(() => {
    const entries = Object.entries(metrics?.domains ?? {});
    return entries.sort((a, b) => b[1] - a[1]);
  }, [metrics]);

  async function refreshOverview(): Promise<void> {
    setLoading(true);
    try {
      const [metricsRes, automationRes, jobsRes, categoriesRes, citiesRes, filesRes] = await Promise.all([
        api.metrics(),
        api.automationStatus(),
        api.jobs(40),
        api.categories(),
        api.cities(),
        api.exportFiles()
      ]);
      setMetrics(metricsRes);
      setAutomation(automationRes);
      setAutoArea(automationRes.settings.area ? String(automationRes.settings.area) : "");
      setAutoIntervalSeconds(String(automationRes.settings.interval_seconds ?? 900));
      setAutoSyncLimit(String(automationRes.settings.sync_limit ?? 100));
      setAutoRdapLimit(String(automationRes.settings.rdap_limit ?? 5));
      setAutoBusinessScoreLimit(String(automationRes.settings.business_score_limit ?? 500));
      setDailyTargetEnabled(Boolean(automationRes.settings.daily_target_enabled));
      setDailyTargetAllowRecycle(Boolean(automationRes.settings.daily_target_allow_recycle ?? true));
      setDailyTargetCount(String(automationRes.settings.daily_target_count ?? 100));
      setDailyTargetMinScore(String(automationRes.settings.daily_target_min_score ?? 40));
      setJobs(jobsRes);
      setCategories(categoriesRes);
      setCities(citiesRes);
      setExportFiles(filesRes);
      setStatusMessage("Overview refreshed");
    } catch (error) {
      setStatusMessage(`Overview refresh failed: ${(error as Error).message}`);
    } finally {
      setLoading(false);
    }
  }

  async function refreshLeads(nextFilters: LeadFilters = filters): Promise<void> {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (nextFilters.minScore.trim()) params.set("min_score", nextFilters.minScore.trim());
      if (nextFilters.category !== "all") params.set("category", nextFilters.category);
      if (nextFilters.city.trim()) params.set("city", nextFilters.city.trim());
      params.set("require_contact", String(nextFilters.requireContact));
      params.set("require_unhosted_domain", String(nextFilters.requireUnhostedDomain));
      params.set("require_domain_qualification", String(nextFilters.requireDomainQualification));
      params.set("require_no_website", String(nextFilters.requireNoWebsite));
      params.set("only_unexported", String(nextFilters.onlyUnexported));
      params.set("limit", nextFilters.limit.trim() || "200");

      const result = await api.businessLeads(params);
      setLeads(result);
      setStatusMessage(`Loaded ${result.returned} leads`);
    } catch (error) {
      setStatusMessage(`Lead query failed: ${(error as Error).message}`);
    } finally {
      setLoading(false);
    }
  }

  async function runPipeline(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setActionLoading(true);
    try {
      const syncLim = Number(pipelineSyncLimit);
      const rdapLim = Number(pipelineRdapLimit);
      const bsLim = Number(pipelineBusinessScoreLimit);
      const bsMin = Number(exportMinScore);
      const result = await api.runPipeline({
        area: pipelineArea || null,
        categories: "all",
        sync_limit: isNaN(syncLim) || pipelineSyncLimit.trim() === "" ? null : syncLim,
        rdap_limit: isNaN(rdapLim) || pipelineRdapLimit.trim() === "" ? null : rdapLim,
        email_limit: 0,
        score_limit: 0,
        business_score_limit: isNaN(bsLim) || pipelineBusinessScoreLimit.trim() === "" ? null : bsLim,
        business_min_score: isNaN(bsMin) || exportMinScore.trim() === "" ? null : bsMin,
        business_platform: exportPlatform,
        business_require_contact: exportRequireContact,
        business_require_unhosted_domain: exportRequireUnhosted,
        business_require_domain_qualification: exportRequireDomainQualification
      });
      const r = result as Record<string, unknown>;
      const parts: string[] = [];
      if (r.imported) parts.push(`${r.imported} imported`);
      if (typeof r.business_scored === "number" && r.business_scored > 0) parts.push(`${r.business_scored} scored`);
      if (r.business_export_path) parts.push("export created");
      setStatusMessage(parts.length ? `Pipeline done: ${parts.join(", ")}` : "Pipeline completed (no new data)");
      await Promise.all([refreshOverview(), refreshLeads()]);
    } catch (error) {
      setStatusMessage(`Pipeline failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function runBusinessScoring(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setActionLoading(true);
    try {
      const sLim = Number(scoreLimit);
      const result = await api.scoreBusinesses({
        limit: isNaN(sLim) || scoreLimit.trim() === "" ? null : sLim,
        scope: pipelineArea || null,
        force_rescore: scoreForceRescore
      });
      setStatusMessage(`Business scoring completed: ${result.processed}`);
      await Promise.all([refreshOverview(), refreshLeads()]);
    } catch (error) {
      setStatusMessage(`Business scoring failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function runBusinessExport(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setActionLoading(true);
    try {
      const result = await api.exportBusinesses({
        platform: exportPlatform,
        min_score: Number(exportMinScore),
        require_contact: exportRequireContact,
        require_unhosted_domain: exportRequireUnhosted,
        require_domain_qualification: exportRequireDomainQualification
      });
      setStatusMessage(result.path ? `Business export created: ${result.path}` : "No rows exported");
      await refreshOverview();
    } catch (error) {
      setStatusMessage(`Business export failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function saveAutomationSettings(): Promise<void> {
    setActionLoading(true);
    try {
      const status = await api.automationUpdateSettings({
        interval_seconds: Number(autoIntervalSeconds),
        area: autoArea.trim() ? autoArea.trim() : null,
        categories: "all",
        sync_limit: Number(autoSyncLimit),
        rdap_limit: Number(autoRdapLimit),
        rdap_statuses: ["new", "skipped", "rdap_error", "dns_error"],
        email_limit: 0,
        score_limit: 0,
        business_score_limit: Number(autoBusinessScoreLimit),
        business_platform: exportPlatform,
        business_min_score: Number(exportMinScore),
        business_require_contact: exportRequireContact,
        business_require_unhosted_domain: exportRequireUnhosted,
        business_require_domain_qualification: exportRequireDomainQualification,
        daily_target_enabled: dailyTargetEnabled,
        daily_target_allow_recycle: dailyTargetAllowRecycle,
        daily_target_count: Number(dailyTargetCount),
        daily_target_min_score: Number(dailyTargetMinScore),
        daily_target_platform_prefix: "daily",
        daily_target_require_contact: exportRequireContact,
        daily_target_require_domain_qualification: exportRequireDomainQualification,
        daily_target_require_unhosted_domain: exportRequireUnhosted
      });
      setAutomation(status);
      setStatusMessage("Automation settings saved");
      await refreshOverview();
    } catch (error) {
      setStatusMessage(`Automation settings failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function startAutomation(): Promise<void> {
    setActionLoading(true);
    try {
      const status = await api.automationStart({
        interval_seconds: Number(autoIntervalSeconds),
        area: autoArea.trim() ? autoArea.trim() : null,
        categories: "all",
        sync_limit: Number(autoSyncLimit),
        rdap_limit: Number(autoRdapLimit),
        rdap_statuses: ["new", "skipped", "rdap_error", "dns_error"],
        email_limit: 0,
        score_limit: 0,
        business_score_limit: Number(autoBusinessScoreLimit),
        business_platform: exportPlatform,
        business_min_score: Number(exportMinScore),
        business_require_contact: exportRequireContact,
        business_require_unhosted_domain: exportRequireUnhosted,
        business_require_domain_qualification: exportRequireDomainQualification,
        daily_target_enabled: dailyTargetEnabled,
        daily_target_allow_recycle: dailyTargetAllowRecycle,
        daily_target_count: Number(dailyTargetCount),
        daily_target_min_score: Number(dailyTargetMinScore),
        daily_target_platform_prefix: "daily",
        daily_target_require_contact: exportRequireContact,
        daily_target_require_domain_qualification: exportRequireDomainQualification,
        daily_target_require_unhosted_domain: exportRequireUnhosted
      });
      setAutomation(status);
      setStatusMessage("Automation started");
      await Promise.all([refreshOverview(), refreshLeads()]);
    } catch (error) {
      setStatusMessage(`Automation start failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function stopAutomation(): Promise<void> {
    setActionLoading(true);
    try {
      const status = await api.automationStop();
      setAutomation(status);
      setStatusMessage("Automation stopped");
      await refreshOverview();
    } catch (error) {
      setStatusMessage(`Automation stop failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function runAutomationNow(): Promise<void> {
    setActionLoading(true);
    try {
      await api.automationRunNow();
      setStatusMessage("Automation cycle started");
      await Promise.all([refreshOverview(), refreshLeads()]);
    } catch (error) {
      setStatusMessage(`Automation run failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  async function generateDailyTargetNow(): Promise<void> {
    setActionLoading(true);
    try {
      const result = await api.automationDailyTargetNow();
      setStatusMessage(`Daily target updated: ${JSON.stringify(result)}`);
      await Promise.all([refreshOverview(), refreshLeads()]);
    } catch (error) {
      setStatusMessage(`Daily target generation failed: ${(error as Error).message}`);
    } finally {
      setActionLoading(false);
    }
  }

  useEffect(() => {
    void Promise.all([refreshOverview(), refreshLeads(defaultFilters)]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void refreshOverview();
      void refreshLeads();
    }, 30000);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <main className="page">
      <header className="hero">
        <div>
          <h1>Domain Lead Command Center</h1>
          <p>
            Find businesses without websites, score leads, and export outreach-ready targets — anywhere in the world.
          </p>
        </div>
        <div className="hero-actions">
          <button onClick={() => void refreshOverview()} disabled={loading || actionLoading}>
            Refresh Overview
          </button>
          <button onClick={() => void refreshLeads()} disabled={loading || actionLoading}>
            Refresh Leads
          </button>
        </div>
      </header>

      <section className={`status-bar ${statusMessage.toLowerCase().includes("failed") || statusMessage.toLowerCase().includes("error") ? "status-error" : ""}`}>
        <strong>Status:</strong> {statusMessage}
        {actionLoading && <span className="spinner"> ⏳ Running...</span>}
      </section>

      <section className="grid">
        <article className="panel">
          <h2>Metrics</h2>
          {metrics ? (
            <div className="metric-grid">
              <div className="metric-card">
                <span>Total Businesses</span>
                <strong>{metrics.businesses.total}</strong>
              </div>
              <div className="metric-card">
                <span>No Website</span>
                <strong>{metrics.businesses.no_website}</strong>
              </div>
              <div className="metric-card">
                <span>Scored Businesses</span>
                <strong>{metrics.businesses.no_website_scored}</strong>
              </div>
              <div className="metric-card">
                <span>Business Exports</span>
                <strong>{metrics.business_exports.total}</strong>
              </div>
              <div className="metric-card">
                <span>Contact Exports</span>
                <strong>{metrics.exports.total}</strong>
              </div>
            </div>
          ) : (
            <p>Loading metrics...</p>
          )}
          <div className="chips">
            {domainStatusBadges.map(([status, count]) => (
              <span key={status} className="chip">
                {status}: {count}
              </span>
            ))}
          </div>
        </article>

        <article className="panel">
          <h2>Actions</h2>
          <form
            onSubmit={(event) => {
              event.preventDefault();
            }}
            className="stack"
          >
            <h3>Always-On Runner</h3>
            <p className="automation-summary">
              <strong>{automation?.running ? "Running" : "Stopped"}</strong>
              {" · "}
              {automation?.busy ? "Cycle in progress" : "Idle"}
              {" · "}
              Runs: {automation?.run_count ?? 0}
            </p>
            <p className="automation-summary">
              Last start: {formatDate(automation?.last_run_started_at)}
              {" · "}
              Last finish: {formatDate(automation?.last_run_finished_at)}
            </p>
            <p className="automation-summary">Last error: {automation?.last_error || "-"}</p>
            <label>
              Runner interval (seconds)
              <input
                type="number"
                min={30}
                value={autoIntervalSeconds}
                onChange={(event) => setAutoIntervalSeconds(event.target.value)}
              />
            </label>
            <label>
              Auto sync limit
              <input
                type="number"
                min={0}
                value={autoSyncLimit}
                onChange={(event) => setAutoSyncLimit(event.target.value)}
              />
            </label>
            <label>
              Auto RDAP limit
              <input
                type="number"
                min={0}
                value={autoRdapLimit}
                onChange={(event) => setAutoRdapLimit(event.target.value)}
              />
            </label>
            <label>
              Auto business score limit
              <input
                type="number"
                min={0}
                value={autoBusinessScoreLimit}
                onChange={(event) => setAutoBusinessScoreLimit(event.target.value)}
              />
            </label>
            <label>
              Import area each cycle (optional)
              <input value={autoArea} onChange={(event) => setAutoArea(event.target.value)} placeholder="e.g. Toronto, Canada" />
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={dailyTargetEnabled}
                onChange={(event) => setDailyTargetEnabled(event.target.checked)}
              />
              Daily target enabled
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={dailyTargetAllowRecycle}
                onChange={(event) => setDailyTargetAllowRecycle(event.target.checked)}
              />
              Allow lead recycle when daily pool is exhausted
            </label>
            <label>
              Daily target count
              <input
                type="number"
                min={1}
                value={dailyTargetCount}
                onChange={(event) => setDailyTargetCount(event.target.value)}
              />
            </label>
            <label>
              Daily target min score
              <input
                type="number"
                value={dailyTargetMinScore}
                onChange={(event) => setDailyTargetMinScore(event.target.value)}
              />
            </label>
            <div className="button-row">
              <button type="button" onClick={() => void saveAutomationSettings()} disabled={actionLoading}>
                Save Settings
              </button>
              <button type="button" onClick={() => void startAutomation()} disabled={actionLoading}>
                Start Always-On
              </button>
              <button type="button" onClick={() => void stopAutomation()} disabled={actionLoading}>
                Stop
              </button>
              <button type="button" onClick={() => void runAutomationNow()} disabled={actionLoading}>
                Run Cycle Now
              </button>
              <button type="button" onClick={() => void generateDailyTargetNow()} disabled={actionLoading}>
                Generate Daily Target
              </button>
            </div>
          </form>

          <form onSubmit={(event) => void runPipeline(event)} className="stack">
            <h3>Run Unified Pipeline</h3>
            <label>
              Area
              <input value={pipelineArea} onChange={(event) => setPipelineArea(event.target.value)} placeholder="e.g. Toronto, Canada" />
            </label>
            <label>
              Sync Limit
              <input
                type="number"
                value={pipelineSyncLimit}
                onChange={(event) => setPipelineSyncLimit(event.target.value)}
              />
            </label>
            <label>
              RDAP Limit
              <input
                type="number"
                value={pipelineRdapLimit}
                onChange={(event) => setPipelineRdapLimit(event.target.value)}
              />
            </label>
            <label>
              Business Score Limit
              <input
                type="number"
                value={pipelineBusinessScoreLimit}
                onChange={(event) => setPipelineBusinessScoreLimit(event.target.value)}
              />
            </label>
            <button type="submit" disabled={actionLoading}>
              {actionLoading ? "⏳ Running Pipeline..." : "Run Pipeline"}
            </button>
          </form>

          <form onSubmit={(event) => void runBusinessScoring(event)} className="stack">
            <h3>Score Businesses</h3>
            <label>
              Score Limit
              <input type="number" value={scoreLimit} onChange={(event) => setScoreLimit(event.target.value)} />
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={scoreForceRescore}
                onChange={(event) => setScoreForceRescore(event.target.checked)}
              />
              Force rescore
            </label>
            <button type="submit" disabled={actionLoading}>
              {actionLoading ? "⏳ Scoring..." : "Score Now"}
            </button>
          </form>

          <form onSubmit={(event) => void runBusinessExport(event)} className="stack">
            <h3>Export Businesses</h3>
            <label>
              Platform Label
              <input value={exportPlatform} onChange={(event) => setExportPlatform(event.target.value)} />
            </label>
            <label>
              Min Score
              <input
                type="number"
                value={exportMinScore}
                onChange={(event) => setExportMinScore(event.target.value)}
              />
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={exportRequireContact}
                onChange={(event) => setExportRequireContact(event.target.checked)}
              />
              Require contact
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={exportRequireUnhosted}
                onChange={(event) => setExportRequireUnhosted(event.target.checked)}
              />
              Require unhosted domain
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={exportRequireDomainQualification}
                onChange={(event) => setExportRequireDomainQualification(event.target.checked)}
              />
              Require domain qualification
            </label>
            <button type="submit" disabled={actionLoading}>
              {actionLoading ? "⏳ Exporting..." : "Export Now"}
            </button>
          </form>
        </article>
      </section>

      <section className="panel">
        <h2>Business Leads</h2>
        <form
          className="filters"
          onSubmit={(event) => {
            event.preventDefault();
            void refreshLeads();
          }}
        >
          <label>
            Min Score
            <input
              type="number"
              value={filters.minScore}
              onChange={(event) => setFilters((prev) => ({ ...prev, minScore: event.target.value }))}
            />
          </label>
          <label>
            Category
            <select
              value={filters.category}
              onChange={(event) => setFilters((prev) => ({ ...prev, category: event.target.value }))}
            >
              <option value="all">all</option>
              {categories.map((entry) => (
                <option key={entry} value={entry}>
                  {entry}
                </option>
              ))}
            </select>
          </label>
          <label>
            City
            <input
              value={filters.city}
              list="city-options"
              onChange={(event) => setFilters((prev) => ({ ...prev, city: event.target.value }))}
            />
            <datalist id="city-options">
              {cities.map((entry) => (
                <option key={entry} value={entry} />
              ))}
            </datalist>
          </label>
          <label>
            Limit
            <input
              type="number"
              value={filters.limit}
              onChange={(event) => setFilters((prev) => ({ ...prev, limit: event.target.value }))}
            />
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={filters.requireContact}
              onChange={(event) => setFilters((prev) => ({ ...prev, requireContact: event.target.checked }))}
            />
            Require contact
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={filters.requireUnhostedDomain}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, requireUnhostedDomain: event.target.checked }))
              }
            />
            Require unhosted domain
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={filters.requireDomainQualification}
              onChange={(event) =>
                setFilters((prev) => ({ ...prev, requireDomainQualification: event.target.checked }))
              }
            />
            Require domain qualification
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={filters.requireNoWebsite}
              onChange={(event) => setFilters((prev) => ({ ...prev, requireNoWebsite: event.target.checked }))}
            />
            No website only
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={filters.onlyUnexported}
              onChange={(event) => setFilters((prev) => ({ ...prev, onlyUnexported: event.target.checked }))}
            />
            Only unexported
          </label>
          <button type="submit" disabled={loading || actionLoading}>
            Apply Filters
          </button>
        </form>

        <p className="table-caption">
          Showing {leads?.returned ?? 0} of {leads?.total_candidates ?? 0} candidates
          {metrics && (
            <span className="total-context">
              {" "}({metrics.businesses.total.toLocaleString()} total businesses, {metrics.businesses.no_website.toLocaleString()} without website, {metrics.businesses.no_website_scored.toLocaleString()} scored)
            </span>
          )}
        </p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Category</th>
                <th>City</th>
                <th>Score</th>
                <th>Email</th>
                <th>Phone</th>
                <th>Domain</th>
                <th>Exported</th>
              </tr>
            </thead>
            <tbody>{leads?.items.map(leadRow)}</tbody>
          </table>
        </div>
      </section>

      <section className="grid">
        <article className="panel">
          <h2>Recent Jobs</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Scope</th>
                  <th>Status</th>
                  <th>Processed</th>
                  <th>Started</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id} className={job.status === "failed" ? "row-error" : ""}>
                    <td>{job.job_name}</td>
                    <td>{job.scope || "-"}</td>
                    <td>{job.status}</td>
                    <td>{job.processed_count}</td>
                    <td>{formatDate(job.started_at)}</td>
                    <td title={job.error || ""}>{job.error ? job.error.substring(0, 80) + (job.error.length > 80 ? "..." : "") : "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className="panel">
          <h2>Export Files</h2>
          <ul className="exports-list">
            {exportFiles.map((file) => (
              <li key={file.name}>
                <a href={`${api.baseUrl}/api/exports/files/${file.name}`} target="_blank" rel="noreferrer">
                  {file.name}
                </a>
                <span>{formatBytes(file.size)}</span>
                <span>{formatDate(new Date(file.modified_at * 1000).toISOString())}</span>
              </li>
            ))}
          </ul>
        </article>
      </section>
    </main>
  );
}
