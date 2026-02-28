import { useState, useEffect } from "react";
import type { AutomationStatus, VerificationSettings } from "../types";
import { api } from "../api";

function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

function formatNum(n: number): string {
  return n.toLocaleString();
}

type Props = {
  automation: AutomationStatus | null;
  setAutomation: (s: AutomationStatus) => void;
  actionLoading: boolean;
  setActionLoading: (v: boolean) => void;
  setStatusMessage: (s: string) => void;
  refresh: () => Promise<void>;
  // Settings state
  autoArea: string; setAutoArea: (v: string) => void;
  autoIntervalSeconds: string; setAutoIntervalSeconds: (v: string) => void;
  autoSyncLimit: string; setAutoSyncLimit: (v: string) => void;
  autoRdapLimit: string; setAutoRdapLimit: (v: string) => void;
  autoBusinessScoreLimit: string; setAutoBusinessScoreLimit: (v: string) => void;
  dailyTargetEnabled: boolean; setDailyTargetEnabled: (v: boolean) => void;
  dailyTargetAllowRecycle: boolean; setDailyTargetAllowRecycle: (v: boolean) => void;
  dailyTargetCount: string; setDailyTargetCount: (v: string) => void;
  dailyTargetMinScore: string; setDailyTargetMinScore: (v: string) => void;
  exportPlatform: string; exportMinScore: string; exportRequireContact: boolean;
  exportRequireUnhosted: boolean; exportRequireDomainQualification: boolean;
};

// Verification layer stat row
function VerifyLayerRow({ label, icon, processed, websites, color }: {
  label: string; icon: string; processed: number; websites: number; color: string;
}) {
  const hitRate = processed > 0 ? ((websites / processed) * 100).toFixed(1) : "0.0";
  return (
    <div className="flex items-center gap-3 py-2">
      <span className="text-base">{icon}</span>
      <span className="text-sm font-medium w-32 truncate">{label}</span>
      <div className="flex-1 flex items-center gap-4">
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-text-secondary">Checked:</span>
          <span className="text-sm font-mono font-semibold tabular-nums">{formatNum(processed)}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-text-secondary">Found:</span>
          <span className={`text-sm font-mono font-semibold tabular-nums ${color}`}>{formatNum(websites)}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-text-secondary">Rate:</span>
          <span className="text-xs font-mono tabular-nums">{hitRate}%</span>
        </div>
      </div>
    </div>
  );
}

export default function AutomationView(p: Props) {
  const { automation, actionLoading, setActionLoading, setStatusMessage, refresh } = p;

  // Verification settings local state
  const v = automation?.verification;
  const vSettings = v?.settings;
  const [vDgBatch, setVDgBatch] = useState("1000");
  const [vDgMinScore, setVDgMinScore] = useState("0");
  const [vSxngBatch, setVSxngBatch] = useState("200");
  const [vSxngMinScore, setVSxngMinScore] = useState("0");
  const [vDdgBatch, setVDdgBatch] = useState("30");
  const [vDdgMinScore, setVDdgMinScore] = useState("30");
  const [vLlmBatch, setVLlmBatch] = useState("30");
  const [vLlmMinScore, setVLlmMinScore] = useState("30");
  const [vGsBatch, setVGsBatch] = useState("20");
  const [vGsMinScore, setVGsMinScore] = useState("30");
  const [vPauseBetween, setVPauseBetween] = useState("3");
  const [vPauseIdle, setVPauseIdle] = useState("60");
  const [vRescoreAfter, setVRescoreAfter] = useState(true);
  const [vSettingsOpen, setVSettingsOpen] = useState(false);

  // Sync verification settings from server
  useEffect(() => {
    if (vSettings) {
      setVDgBatch(String(vSettings.domain_guess_batch));
      setVDgMinScore(String(vSettings.domain_guess_min_score));
      setVSxngBatch(String(vSettings.searxng_batch));
      setVSxngMinScore(String(vSettings.searxng_min_score));
      setVDdgBatch(String(vSettings.ddg_batch));
      setVDdgMinScore(String(vSettings.ddg_min_score));
      setVLlmBatch(String(vSettings.llm_batch));
      setVLlmMinScore(String(vSettings.llm_min_score));
      setVGsBatch(String(vSettings.google_search_batch));
      setVGsMinScore(String(vSettings.google_search_min_score));
      setVPauseBetween(String(vSettings.pause_between_batches));
      setVPauseIdle(String(vSettings.pause_when_idle));
      setVRescoreAfter(vSettings.rescore_after_batch);
    }
  }, [vSettings]);

  function buildPayload() {
    return {
      interval_seconds: Number(p.autoIntervalSeconds),
      area: p.autoArea.trim() || null,
      categories: "all",
      sync_limit: Number(p.autoSyncLimit),
      rdap_limit: Number(p.autoRdapLimit),
      rdap_statuses: ["new", "skipped", "rdap_error", "dns_error"],
      email_limit: 0, score_limit: 0,
      business_score_limit: Number(p.autoBusinessScoreLimit),
      business_platform: p.exportPlatform,
      business_min_score: Number(p.exportMinScore),
      business_require_contact: p.exportRequireContact,
      business_require_unhosted_domain: p.exportRequireUnhosted,
      business_require_domain_qualification: p.exportRequireDomainQualification,
      daily_target_enabled: p.dailyTargetEnabled,
      daily_target_allow_recycle: p.dailyTargetAllowRecycle,
      daily_target_count: Number(p.dailyTargetCount),
      daily_target_min_score: Number(p.dailyTargetMinScore),
      daily_target_platform_prefix: "daily",
      daily_target_require_contact: p.exportRequireContact,
      daily_target_require_domain_qualification: p.exportRequireDomainQualification,
      daily_target_require_unhosted_domain: p.exportRequireUnhosted,
    };
  }

  function buildVerifyPayload(): Partial<VerificationSettings> {
    return {
      domain_guess_batch: Number(vDgBatch),
      domain_guess_min_score: Number(vDgMinScore),
      searxng_batch: Number(vSxngBatch),
      searxng_min_score: Number(vSxngMinScore),
      ddg_batch: Number(vDdgBatch),
      ddg_min_score: Number(vDdgMinScore),
      llm_batch: Number(vLlmBatch),
      llm_min_score: Number(vLlmMinScore),
      google_search_batch: Number(vGsBatch),
      google_search_min_score: Number(vGsMinScore),
      rescore_after_batch: vRescoreAfter,
      pause_between_batches: Number(vPauseBetween),
      pause_when_idle: Number(vPauseIdle),
    };
  }

  async function act(label: string, fn: () => Promise<AutomationStatus | Record<string, unknown>>) {
    setActionLoading(true);
    try {
      const status = await fn();
      if ("running" in status && "settings" in status) p.setAutomation(status as AutomationStatus);
      setStatusMessage(`${label}: done`);
      await refresh();
    } catch (e) { setStatusMessage(`${label} failed: ${(e as Error).message}`); }
    finally { setActionLoading(false); }
  }

  const isRunning = automation?.running ?? false;
  const isVerifyRunning = v?.running ?? false;
  const totals = v?.totals;

  const totalProcessed = totals
    ? totals.domain_guess_processed + (totals.searxng_processed ?? 0) + totals.ddg_processed + totals.llm_processed + totals.google_search_processed
    : 0;
  const totalWebsites = totals
    ? totals.domain_guess_websites + (totals.searxng_websites ?? 0) + totals.ddg_websites + totals.llm_websites + totals.google_search_websites
    : 0;

  return (
    <div className="space-y-6">
      {/* ─── Continuous Verification ─── */}
      <div className="bg-bg-card rounded-xl border-2 border-emerald-200 p-5">
        <div className="flex items-center gap-4 mb-4">
          <div className={`w-3.5 h-3.5 rounded-full ring-4 ${isVerifyRunning ? "bg-signal-green ring-emerald-100 animate-pulse" : "bg-gray-400 ring-gray-100"}`} />
          <div>
            <h3 className="font-bold text-base">Continuous Verification</h3>
            <p className="text-xs text-text-secondary">Cycles through all verification layers automatically</p>
          </div>
          <div className="ml-auto flex items-center gap-2">
            {isVerifyRunning ? (
              <button
                onClick={() => void act("Stop Verification", () => api.stopVerification())}
                disabled={actionLoading}
                className="rounded-lg bg-signal-red text-white font-semibold py-2 px-5 text-sm hover:bg-red-700 disabled:opacity-50 transition-colors"
              >
                Stop
              </button>
            ) : (
              <button
                onClick={() => void act("Start Verification", () => api.startVerification(buildVerifyPayload()))}
                disabled={actionLoading}
                className="rounded-lg bg-signal-green text-white font-semibold py-2 px-5 text-sm hover:bg-emerald-700 disabled:opacity-50 transition-colors"
              >
                Start
              </button>
            )}
          </div>
        </div>

        {/* Live totals */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          <div className="bg-emerald-50 rounded-lg p-3 text-center">
            <p className="text-2xl font-bold tabular-nums text-emerald-800">{formatNum(totalProcessed)}</p>
            <p className="text-xs text-emerald-600 font-medium">Total Checked</p>
          </div>
          <div className="bg-blue-50 rounded-lg p-3 text-center">
            <p className="text-2xl font-bold tabular-nums text-blue-800">{formatNum(totalWebsites)}</p>
            <p className="text-xs text-blue-600 font-medium">Websites Found</p>
          </div>
          <div className="bg-amber-50 rounded-lg p-3 text-center">
            <p className="text-2xl font-bold tabular-nums text-amber-800">{v?.batch_count ?? 0}</p>
            <p className="text-xs text-amber-600 font-medium">Full Cycles</p>
          </div>
          <div className="bg-purple-50 rounded-lg p-3 text-center">
            <p className="text-2xl font-bold tabular-nums text-purple-800">{formatNum(totals?.rescored ?? 0)}</p>
            <p className="text-xs text-purple-600 font-medium">Rescored</p>
          </div>
        </div>

        {/* Per-layer breakdown */}
        <div className="bg-white/60 rounded-lg border border-border/50 divide-y divide-border/30 px-4">
          <VerifyLayerRow
            label="Domain Guess" icon="🔍"
            processed={totals?.domain_guess_processed ?? 0}
            websites={totals?.domain_guess_websites ?? 0}
            color="text-signal-blue"
          />
          <VerifyLayerRow
            label="SearXNG" icon="🌐"
            processed={totals?.searxng_processed ?? 0}
            websites={totals?.searxng_websites ?? 0}
            color="text-indigo-700"
          />
          <VerifyLayerRow
            label="LLM (Groq)" icon="🤖"
            processed={totals?.llm_processed ?? 0}
            websites={totals?.llm_websites ?? 0}
            color="text-signal-green"
          />
          <VerifyLayerRow
            label="DDG Search" icon="🦆"
            processed={totals?.ddg_processed ?? 0}
            websites={totals?.ddg_websites ?? 0}
            color="text-amber-700"
          />
          <VerifyLayerRow
            label="Google Search" icon="🔎"
            processed={totals?.google_search_processed ?? 0}
            websites={totals?.google_search_websites ?? 0}
            color="text-teal-700"
          />
        </div>

        {/* Meta info */}
        <div className="flex flex-wrap gap-4 mt-3 text-xs text-text-secondary">
          <span>Started: {formatDate(v?.last_started_at)}</span>
          {v?.last_error && <span className="text-signal-red">Error: {v.last_error}</span>}
        </div>

        {/* Collapsible verification settings */}
        <div className="mt-4">
          <button
            onClick={() => setVSettingsOpen(!vSettingsOpen)}
            className="flex items-center gap-2 text-xs font-semibold text-text-secondary hover:text-text-primary transition-colors"
          >
            <svg className={`w-3.5 h-3.5 transition-transform ${vSettingsOpen ? "rotate-90" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
            Verification Settings
          </button>
          {vSettingsOpen && (
            <div className="mt-3 space-y-3 pl-1">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <label className="text-xs text-text-secondary">DG Batch<input type="number" min={1} max={5000} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vDgBatch} onChange={(e) => setVDgBatch(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">DG Min Score<input type="number" min={0} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vDgMinScore} onChange={(e) => setVDgMinScore(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">SearXNG Batch<input type="number" min={1} max={2000} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vSxngBatch} onChange={(e) => setVSxngBatch(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">SearXNG Min Score<input type="number" min={0} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vSxngMinScore} onChange={(e) => setVSxngMinScore(e.target.value)} /></label>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <label className="text-xs text-text-secondary">DDG Batch<input type="number" min={1} max={1000} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vDdgBatch} onChange={(e) => setVDdgBatch(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">DDG Min Score<input type="number" min={0} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vDdgMinScore} onChange={(e) => setVDdgMinScore(e.target.value)} /></label>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <label className="text-xs text-text-secondary">LLM Batch<input type="number" min={1} max={500} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vLlmBatch} onChange={(e) => setVLlmBatch(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">LLM Min Score<input type="number" min={0} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vLlmMinScore} onChange={(e) => setVLlmMinScore(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">GS Batch<input type="number" min={1} max={500} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vGsBatch} onChange={(e) => setVGsBatch(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">GS Min Score<input type="number" min={0} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vGsMinScore} onChange={(e) => setVGsMinScore(e.target.value)} /></label>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <label className="text-xs text-text-secondary">Pause between (sec)<input type="number" min={1} max={300} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vPauseBetween} onChange={(e) => setVPauseBetween(e.target.value)} /></label>
                <label className="text-xs text-text-secondary">Idle pause (sec)<input type="number" min={10} max={3600} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={vPauseIdle} onChange={(e) => setVPauseIdle(e.target.value)} /></label>
                <label className="flex items-center gap-2 text-xs text-text-secondary cursor-pointer pt-4 col-span-2">
                  <input type="checkbox" className="rounded" checked={vRescoreAfter} onChange={(e) => setVRescoreAfter(e.target.checked)} />
                  Rescore after each batch
                </label>
              </div>
              <button
                onClick={() => void act("Save Verify Settings", () => api.updateVerificationSettings(buildVerifyPayload()))}
                disabled={actionLoading}
                className="rounded-lg bg-accent text-white font-semibold py-2 px-6 text-sm hover:bg-accent-hover disabled:opacity-50"
              >
                Save Verification Settings
              </button>
            </div>
          )}
        </div>
      </div>

      {/* ─── Pipeline Automation ─── */}
      <div className="bg-bg-card rounded-xl border border-border p-5">
        <div className="flex items-center gap-4 mb-4">
          <div className={`w-3 h-3 rounded-full ${isRunning ? "bg-signal-green animate-pulse" : "bg-signal-red"}`} />
          <div>
            <h3 className="font-semibold">Pipeline Automation</h3>
            <p className="text-xs text-text-secondary">Runs full pipeline cycles: sync, RDAP, verify, score, export</p>
          </div>
          {automation?.busy && <span className="text-xs bg-signal-amber text-white px-2 py-0.5 rounded-full">Cycle in progress</span>}
          <span className="text-xs text-text-secondary ml-auto">Runs: {automation?.run_count ?? 0}</span>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs text-text-secondary">
          <div><span className="block font-medium text-text-primary">Last Started</span>{formatDate(automation?.last_run_started_at)}</div>
          <div><span className="block font-medium text-text-primary">Last Finished</span>{formatDate(automation?.last_run_finished_at)}</div>
          <div><span className="block font-medium text-text-primary">Last Error</span><span className="text-signal-red">{automation?.last_error || "-"}</span></div>
        </div>

        <div className="flex flex-wrap gap-2 mt-4">
          <button onClick={() => void act("Start", () => api.automationStart(buildPayload()))} disabled={actionLoading} className="rounded-lg bg-signal-green text-white font-semibold py-2 px-4 text-sm hover:bg-emerald-700 disabled:opacity-50">Start</button>
          <button onClick={() => void act("Stop", () => api.automationStop())} disabled={actionLoading} className="rounded-lg bg-signal-red text-white font-semibold py-2 px-4 text-sm hover:bg-red-700 disabled:opacity-50">Stop</button>
          <button onClick={() => void act("Run Cycle", () => api.automationRunNow())} disabled={actionLoading} className="rounded-lg bg-accent text-white font-semibold py-2 px-4 text-sm hover:bg-accent-hover disabled:opacity-50">Run Cycle Now</button>
          <button onClick={() => void act("Daily Target", () => api.automationDailyTargetNow())} disabled={actionLoading} className="rounded-lg bg-signal-purple text-white font-semibold py-2 px-4 text-sm hover:bg-purple-700 disabled:opacity-50">Generate Daily Target</button>
        </div>
      </div>

      {/* ─── Pipeline Settings ─── */}
      <div className="bg-bg-card rounded-xl border border-border p-5 space-y-4">
        <h3 className="font-semibold text-sm">Pipeline Settings</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <label className="text-xs text-text-secondary">Interval (sec)<input type="number" min={30} className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.autoIntervalSeconds} onChange={(e) => p.setAutoIntervalSeconds(e.target.value)} /></label>
          <label className="text-xs text-text-secondary">Sync limit<input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.autoSyncLimit} onChange={(e) => p.setAutoSyncLimit(e.target.value)} /></label>
          <label className="text-xs text-text-secondary">RDAP limit<input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.autoRdapLimit} onChange={(e) => p.setAutoRdapLimit(e.target.value)} /></label>
          <label className="text-xs text-text-secondary">Score limit<input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.autoBusinessScoreLimit} onChange={(e) => p.setAutoBusinessScoreLimit(e.target.value)} /></label>
        </div>
        <label className="block text-xs text-text-secondary">Import area each cycle<input className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={p.autoArea} onChange={(e) => p.setAutoArea(e.target.value)} placeholder="e.g. Toronto, Canada" /></label>

        <h4 className="text-xs font-semibold text-text-secondary uppercase tracking-wider pt-2">Daily Target</h4>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <label className="flex items-center gap-2 text-xs text-text-secondary col-span-2 cursor-pointer"><input type="checkbox" className="rounded" checked={p.dailyTargetEnabled} onChange={(e) => p.setDailyTargetEnabled(e.target.checked)} /> Daily target enabled</label>
          <label className="flex items-center gap-2 text-xs text-text-secondary col-span-2 cursor-pointer"><input type="checkbox" className="rounded" checked={p.dailyTargetAllowRecycle} onChange={(e) => p.setDailyTargetAllowRecycle(e.target.checked)} /> Allow lead recycle</label>
          <label className="text-xs text-text-secondary">Count<input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.dailyTargetCount} onChange={(e) => p.setDailyTargetCount(e.target.value)} /></label>
          <label className="text-xs text-text-secondary">Min score<input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={p.dailyTargetMinScore} onChange={(e) => p.setDailyTargetMinScore(e.target.value)} /></label>
        </div>
        <button onClick={() => void act("Save Settings", () => api.automationUpdateSettings(buildPayload()))} disabled={actionLoading} className="rounded-lg bg-accent text-white font-semibold py-2 px-6 text-sm hover:bg-accent-hover disabled:opacity-50">Save Pipeline Settings</button>
      </div>
    </div>
  );
}
