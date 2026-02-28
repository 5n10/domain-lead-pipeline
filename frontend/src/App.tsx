import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "./api";
import type { AutomationStatus } from "./types";
import DashboardView from "./components/DashboardView";
import LeadsView, { defaultFilters, type LeadFilters } from "./components/LeadsView";
import ActionsView from "./components/ActionsView";
import AutomationView from "./components/AutomationView";
import JobsView from "./components/JobsView";
import ExportsView from "./components/ExportsView";
import { useEffect } from "react";

type View = "dashboard" | "leads" | "actions" | "automation" | "jobs" | "exports";

const NAV_ITEMS: { key: View; label: string; icon: string }[] = [
  { key: "dashboard", label: "Dashboard", icon: "📊" },
  { key: "leads", label: "Leads", icon: "👥" },
  { key: "actions", label: "Actions", icon: "⚡" },
  { key: "automation", label: "Automation", icon: "🔁" },
  { key: "jobs", label: "Jobs", icon: "📋" },
  { key: "exports", label: "Exports", icon: "📦" },
];

export default function App() {
  const [view, setView] = useState<View>("dashboard");
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Data state via React Query
  const { data: metrics } = useQuery({ queryKey: ["metrics"], queryFn: api.metrics, refetchInterval: 30000 });
  const { data: automation } = useQuery({ queryKey: ["automation"], queryFn: api.automationStatus, refetchInterval: 10000 });
  const { data: jobs } = useQuery({ queryKey: ["jobs"], queryFn: () => api.jobs(40), refetchInterval: 30000 });
  const { data: categories } = useQuery({ queryKey: ["categories"], queryFn: api.categories });
  const { data: cities } = useQuery({ queryKey: ["cities"], queryFn: api.cities });
  const { data: exportFiles } = useQuery({ queryKey: ["exportFiles"], queryFn: api.exportFiles, refetchInterval: 30000 });

  const [filters, setFilters] = useState<LeadFilters>(defaultFilters);

  const { data: leads, error: leadsError } = useQuery({
    queryKey: ["leads", filters],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (filters.minScore.trim()) params.set("min_score", filters.minScore.trim());
      if (filters.category !== "all") params.set("category", filters.category);
      if (filters.city.trim()) params.set("city", filters.city.trim());
      if (filters.minConfidence) params.set("min_confidence", filters.minConfidence);
      params.set("require_contact", String(filters.requireContact));
      params.set("require_unhosted_domain", String(filters.requireUnhostedDomain));
      params.set("require_domain_qualification", String(filters.requireDomainQualification));
      params.set("require_no_website", String(filters.requireNoWebsite));
      params.set("exclude_hosted_email_domain", String(filters.excludeHostedEmailDomain));
      params.set("only_unexported", String(filters.onlyUnexported));
      params.set("only_verified", String(filters.onlyVerified));
      params.set("limit", filters.limit.trim() || "200");
      return api.businessLeads(params);
    }
  });

  // UI state
  const [statusMessage, setStatusMessage] = useState("Ready");
  const [actionLoading, setActionLoading] = useState(false);

  const queryClient = useQueryClient();
  const loading = !metrics || !automation || !jobs;

  // Settings state
  const [exportPlatform, setExportPlatform] = useState("csv_business");
  const [exportMinScore, setExportMinScore] = useState("40");
  const [exportRequireContact, setExportRequireContact] = useState(true);
  const [exportRequireUnhosted, setExportRequireUnhosted] = useState(false);
  const [exportRequireDomainQualification, setExportRequireDomainQualification] = useState(false);
  const [autoArea, setAutoArea] = useState("");
  const [autoIntervalSeconds, setAutoIntervalSeconds] = useState("900");
  const [autoSyncLimit, setAutoSyncLimit] = useState("2000");
  const [autoRdapLimit, setAutoRdapLimit] = useState("50");
  const [autoBusinessScoreLimit, setAutoBusinessScoreLimit] = useState("500");
  const [dailyTargetEnabled, setDailyTargetEnabled] = useState(true);
  const [dailyTargetAllowRecycle, setDailyTargetAllowRecycle] = useState(true);
  const [dailyTargetCount, setDailyTargetCount] = useState("100");
  const [dailyTargetMinScore, setDailyTargetMinScore] = useState("40");

  useEffect(() => {
    if (automation) {
      setAutoArea(automation.settings.area ? String(automation.settings.area) : "");
      setAutoIntervalSeconds(String(automation.settings.interval_seconds ?? 900));
      setAutoSyncLimit(String(automation.settings.sync_limit ?? 100));
      setAutoRdapLimit(String(automation.settings.rdap_limit ?? 5));
      setAutoBusinessScoreLimit(String(automation.settings.business_score_limit ?? 500));
      setDailyTargetEnabled(Boolean(automation.settings.daily_target_enabled));
      setDailyTargetAllowRecycle(Boolean(automation.settings.daily_target_allow_recycle ?? true));
      setDailyTargetCount(String(automation.settings.daily_target_count ?? 100));
      setDailyTargetMinScore(String(automation.settings.daily_target_min_score ?? 40));
    }
  }, [automation]);

  useEffect(() => {
    if (leadsError) {
      setStatusMessage(`Lead query failed: ${(leadsError as Error).message}`);
    } else {
      setStatusMessage("Ready");
    }
  }, [leadsError]);

  async function fullRefresh() {
    await queryClient.invalidateQueries();
  }

  const isError = statusMessage.toLowerCase().includes("failed") || statusMessage.toLowerCase().includes("error");

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div className="fixed inset-0 bg-black/40 z-30 lg:hidden" onClick={() => setSidebarOpen(false)} />
      )}

      {/* Sidebar */}
      <aside className={`fixed lg:static inset-y-0 left-0 z-40 w-56 bg-bg-sidebar flex flex-col transition-transform lg:translate-x-0 ${sidebarOpen ? "translate-x-0" : "-translate-x-full"}`}>
        <div className="p-5 pb-3">
          <h1 className="text-base font-bold text-white tracking-tight">Domain Leads</h1>
          <p className="text-[0.65rem] text-text-sidebar mt-0.5">Command Center</p>
        </div>
        <nav className="flex-1 px-3 space-y-0.5">
          {NAV_ITEMS.map(({ key, label, icon }) => (
            <button
              key={key}
              onClick={() => { setView(key); setSidebarOpen(false); }}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${view === key ? "bg-sidebar-active text-white font-semibold" : "text-text-sidebar hover:bg-sidebar-hover"
                }`}
            >
              <span className="text-base">{icon}</span>
              {label}
            </button>
          ))}
        </nav>
        {/* Status indicator */}
        <div className="p-4 border-t border-white/10 space-y-2">
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${automation?.verification?.running ? "bg-signal-green animate-pulse" : "bg-gray-500"}`} />
            <span className="text-xs text-text-sidebar">{automation?.verification?.running ? "Verification ON" : "Verification OFF"}</span>
          </div>
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${automation?.running ? "bg-signal-green animate-pulse" : "bg-signal-red"}`} />
            <span className="text-xs text-text-sidebar">{automation?.running ? "Pipeline ON" : "Pipeline OFF"}</span>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Top bar */}
        <header className="bg-bg-card border-b border-border px-4 py-3 flex items-center gap-3 shrink-0">
          <button className="lg:hidden p-1" onClick={() => setSidebarOpen(!sidebarOpen)}>
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" /></svg>
          </button>
          <h2 className="text-sm font-semibold capitalize">{view}</h2>
          <div className="flex-1" />
          <div className={`text-xs px-3 py-1 rounded-full truncate max-w-[50%] ${isError ? "bg-red-100 text-signal-red" : "bg-amber-50 text-text-secondary"}`}>
            {statusMessage}
            {actionLoading && <span className="ml-1 animate-pulse">...</span>}
          </div>
          <button
            onClick={() => void fullRefresh()}
            disabled={loading}
            className="rounded-lg bg-accent text-white text-xs font-semibold px-3 py-1.5 hover:bg-accent-hover disabled:opacity-50 transition-colors"
          >
            {loading ? "..." : "Refresh"}
          </button>
        </header>

        {/* View content */}
        <div className="flex-1 overflow-y-auto p-4 lg:p-6">
          {view === "dashboard" && <DashboardView metrics={metrics ?? null} automation={automation ?? null} />}
          {view === "leads" && (
            <LeadsView
              leads={leads ?? null} metrics={metrics ?? null} filters={filters} categories={categories ?? []} cities={cities ?? []} loading={loading || actionLoading}
              onFiltersChange={setFilters}
              onApply={() => void queryClient.invalidateQueries({ queryKey: ["leads"] })}
            />
          )}
          {view === "actions" && (
            <ActionsView
              actionLoading={actionLoading} setActionLoading={setActionLoading} setStatusMessage={setStatusMessage} refresh={fullRefresh}
              exportPlatform={exportPlatform} setExportPlatform={setExportPlatform}
              exportMinScore={exportMinScore} setExportMinScore={setExportMinScore}
              exportRequireContact={exportRequireContact} setExportRequireContact={setExportRequireContact}
              exportRequireUnhosted={exportRequireUnhosted} setExportRequireUnhosted={setExportRequireUnhosted}
              exportRequireDomainQualification={exportRequireDomainQualification} setExportRequireDomainQualification={setExportRequireDomainQualification}
            />
          )}
          {view === "automation" && (
            <AutomationView
              automation={automation ?? null} setAutomation={(s: AutomationStatus) => queryClient.setQueryData(["automation"], s)}
              actionLoading={actionLoading} setActionLoading={setActionLoading} setStatusMessage={setStatusMessage} refresh={fullRefresh}
              autoArea={autoArea} setAutoArea={setAutoArea}
              autoIntervalSeconds={autoIntervalSeconds} setAutoIntervalSeconds={setAutoIntervalSeconds}
              autoSyncLimit={autoSyncLimit} setAutoSyncLimit={setAutoSyncLimit}
              autoRdapLimit={autoRdapLimit} setAutoRdapLimit={setAutoRdapLimit}
              autoBusinessScoreLimit={autoBusinessScoreLimit} setAutoBusinessScoreLimit={setAutoBusinessScoreLimit}
              dailyTargetEnabled={dailyTargetEnabled} setDailyTargetEnabled={setDailyTargetEnabled}
              dailyTargetAllowRecycle={dailyTargetAllowRecycle} setDailyTargetAllowRecycle={setDailyTargetAllowRecycle}
              dailyTargetCount={dailyTargetCount} setDailyTargetCount={setDailyTargetCount}
              dailyTargetMinScore={dailyTargetMinScore} setDailyTargetMinScore={setDailyTargetMinScore}
              exportPlatform={exportPlatform} exportMinScore={exportMinScore} exportRequireContact={exportRequireContact}
              exportRequireUnhosted={exportRequireUnhosted} exportRequireDomainQualification={exportRequireDomainQualification}
            />
          )}
          {view === "jobs" && <JobsView jobs={jobs ?? []} />}
          {view === "exports" && <ExportsView files={exportFiles ?? []} />}
        </div>
      </main>
    </div>
  );
}
