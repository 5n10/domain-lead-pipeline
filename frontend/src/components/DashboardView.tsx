import { useMemo } from "react";
import type { AutomationStatus, Metrics, VerificationTotals } from "../types";
import MetricCard from "./MetricCard";

type Props = {
  metrics: Metrics | null;
  automation?: AutomationStatus | null;
};

const verifyColors: Record<string, string> = {
  any_source: "bg-emerald-700",
  domain_guess: "bg-signal-blue",
  searxng: "bg-indigo-600",
  ddg: "bg-amber-700",
  google_places: "bg-signal-green",
  foursquare: "bg-signal-purple",
  google_search: "bg-teal-600",
  llm: "bg-signal-green",
};

const verifyLabels: Record<string, string> = {
  any_source: "Any Verified",
  domain_guess: "Domain Guess",
  searxng: "SearXNG",
  ddg: "DDG Search",
  google_places: "Google Places",
  foursquare: "Foursquare",
  google_search: "Google Search",
  llm: "LLM (AI)",
};

function LiveVerificationBanner({ automation }: { automation: AutomationStatus | null | undefined }) {
  const v = automation?.verification;
  if (!v) return null;

  const totals: VerificationTotals = v.totals;
  const totalProcessed = totals.domain_guess_processed + (totals.searxng_processed ?? 0) + totals.ddg_processed + totals.llm_processed + totals.google_search_processed;
  const totalWebsites = totals.domain_guess_websites + (totals.searxng_websites ?? 0) + totals.ddg_websites + totals.llm_websites + totals.google_search_websites;

  return (
    <div className={`rounded-xl border-2 p-4 ${v.running ? "border-emerald-200 bg-emerald-50/50" : "border-gray-200 bg-gray-50/50"}`}>
      <div className="flex items-center gap-3 mb-3">
        <div className={`w-2.5 h-2.5 rounded-full ${v.running ? "bg-signal-green animate-pulse" : "bg-gray-400"}`} />
        <h3 className="font-semibold text-sm">{v.running ? "Verification Running" : "Verification Stopped"}</h3>
        {v.running && v.batch_count > 0 && (
          <span className="text-xs bg-emerald-100 text-emerald-800 px-2 py-0.5 rounded-full font-medium">{v.batch_count} cycles</span>
        )}
      </div>
      <div className="grid grid-cols-3 md:grid-cols-7 gap-2">
        {[
          { label: "DG", val: totals.domain_guess_processed, found: totals.domain_guess_websites, color: "text-blue-700" },
          { label: "SearXNG", val: totals.searxng_processed ?? 0, found: totals.searxng_websites ?? 0, color: "text-indigo-700" },
          { label: "LLM", val: totals.llm_processed, found: totals.llm_websites, color: "text-green-700" },
          { label: "DDG", val: totals.ddg_processed, found: totals.ddg_websites, color: "text-amber-700" },
          { label: "GS", val: totals.google_search_processed, found: totals.google_search_websites, color: "text-teal-700" },
          { label: "Total", val: totalProcessed, found: totalWebsites, color: "text-gray-900" },
          { label: "Rescored", val: totals.rescored, found: null, color: "text-purple-700" },
        ].map(({ label, val, found, color }) => (
          <div key={label} className="text-center">
            <p className={`text-lg font-bold tabular-nums ${color}`}>{val.toLocaleString()}</p>
            <p className="text-[0.6rem] text-text-secondary uppercase tracking-wider">{label}</p>
            {found !== null && <p className="text-[0.6rem] text-text-secondary">{found} found</p>}
          </div>
        ))}
      </div>
    </div>
  );
}

function VerificationFunnel({ metrics }: { metrics: Metrics }) {
  const stages = useMemo(() => {
    const totalBiz = metrics.businesses.total;
    const noWebsite = metrics.businesses.no_website;
    const anyVerified = metrics.verification?.any_source ?? 0;
    const conf = metrics.confidence_distribution ?? { high: 0, medium: 0, low: 0, unverified: 0 };
    const confident = conf.high + conf.medium;
    const exported = metrics.business_exports.total;

    return [
      { label: "Total", value: totalBiz, color: "bg-gray-400", desc: "All businesses in DB" },
      { label: "No Website", value: noWebsite, color: "bg-amber-500", desc: "Missing website tag" },
      { label: "Verified", value: anyVerified, color: "bg-blue-500", desc: "Checked by 1+ source" },
      { label: "Confident", value: confident, color: "bg-emerald-500", desc: "High/medium confidence" },
      { label: "Exported", value: exported, color: "bg-purple-500", desc: "Sent to outreach" },
    ];
  }, [metrics]);

  const maxVal = Math.max(...stages.map((s) => s.value), 1);

  return (
    <div className="bg-bg-card rounded-xl border border-border p-5">
      <h3 className="font-semibold mb-4 text-sm">Verification Pipeline Funnel</h3>
      <div className="space-y-2">
        {stages.map((stage, i) => {
          const pct = (stage.value / maxVal) * 100;
          const prevVal = i > 0 ? stages[i - 1].value : 0;
          const convRate = i > 0 && prevVal > 0 && stage.value <= prevVal
            ? ((stage.value / prevVal) * 100).toFixed(1)
            : null;
          return (
            <div key={stage.label} className="flex items-center gap-3">
              <span className="text-xs text-text-secondary w-20 text-right shrink-0">{stage.label}</span>
              <div className="flex-1 relative">
                <div className="h-7 bg-gray-100 rounded-md overflow-hidden">
                  <div
                    className={`h-full ${stage.color} rounded-md transition-all duration-500 flex items-center px-2`}
                    style={{ width: `${Math.max(pct, stage.value > 0 ? 3 : 0)}%` }}
                  >
                    {pct > 15 && (
                      <span className="text-white text-xs font-semibold tabular-nums">
                        {stage.value.toLocaleString()}
                      </span>
                    )}
                  </div>
                </div>
              </div>
              <span className="text-xs font-mono tabular-nums w-16 text-right shrink-0">
                {pct <= 15 ? stage.value.toLocaleString() : ""}
              </span>
              <span className="text-[0.6rem] text-text-secondary w-12 text-right shrink-0">
                {convRate ? `${convRate}%` : ""}
              </span>
            </div>
          );
        })}
      </div>
      <p className="text-[0.6rem] text-text-secondary mt-2 text-right">% = conversion from previous stage</p>
    </div>
  );
}

export default function DashboardView({ metrics, automation }: Props) {
  if (!metrics) return <p className="text-text-secondary p-6">Loading metrics...</p>;

  const confDist = metrics.confidence_distribution ?? { high: 0, medium: 0, low: 0, unverified: 0 };
  const confTotal = confDist.high + confDist.medium + confDist.low + confDist.unverified || 1;
  const domainEntries = Object.entries(metrics.domains ?? {}).sort((a, b) => b[1] - a[1]);

  return (
    <div className="space-y-6">
      {/* Live Verification Status */}
      <LiveVerificationBanner automation={automation} />

      {/* Key Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard label="Total Businesses" value={metrics.businesses.total} accent />
        <MetricCard label="No Website" value={metrics.businesses.no_website} />
        <MetricCard label="Scored (No Website)" value={metrics.businesses.no_website_scored} />
        <MetricCard label="Business Exports" value={metrics.business_exports.total} />
      </div>

      {/* Verification Funnel */}
      <VerificationFunnel metrics={metrics} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Verification Coverage */}
        <div className="bg-bg-card rounded-xl border border-border p-5">
          <h3 className="font-semibold mb-3 text-sm">Verification Coverage</h3>
          <div className="flex flex-wrap gap-2">
            {Object.entries(metrics.verification ?? {}).map(([source, count]) => (
              <span
                key={source}
                className={`${verifyColors[source] ?? "bg-gray-500"} text-white text-xs font-semibold px-3 py-1.5 rounded-full`}
              >
                {verifyLabels[source] ?? source}: {(count as number).toLocaleString()}
              </span>
            ))}
          </div>
          {metrics.verification_details && (
            <div className="mt-3 text-xs text-text-secondary space-y-1">
              {(metrics.verification_details.searxng_conclusive ?? 0) + (metrics.verification_details.searxng_no_results ?? 0) > 0 && (
                <div>SearXNG breakdown: {metrics.verification_details.searxng_conclusive ?? 0} conclusive, {metrics.verification_details.searxng_no_results ?? 0} inconclusive</div>
              )}
              <div>DDG breakdown: {metrics.verification_details.ddg_conclusive} conclusive, {metrics.verification_details.ddg_no_results} inconclusive</div>
              {metrics.verification_details.llm_conclusive !== undefined && (
                <div>LLM breakdown: {metrics.verification_details.llm_conclusive} conclusive, {metrics.verification_details.llm_not_sure} inconclusive</div>
              )}
            </div>
          )}
        </div>

        {/* Confidence Distribution */}
        <div className="bg-bg-card rounded-xl border border-border p-5">
          <h3 className="font-semibold mb-3 text-sm">Confidence Distribution</h3>
          <div className="space-y-2.5">
            {[
              { key: "high", label: "High", color: "bg-conf-high", count: confDist.high },
              { key: "medium", label: "Medium", color: "bg-conf-medium", count: confDist.medium },
              { key: "low", label: "Low", color: "bg-conf-low", count: confDist.low },
              { key: "unverified", label: "Unverified", color: "bg-conf-unverified", count: confDist.unverified },
            ].map((item) => (
              <div key={item.key} className="flex items-center gap-3">
                <span className="text-xs text-text-secondary w-20">{item.label}</span>
                <div className="flex-1 h-5 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className={`h-full ${item.color} rounded-full transition-all`}
                    style={{ width: `${Math.max((item.count / confTotal) * 100, item.count > 0 ? 2 : 0)}%` }}
                  />
                </div>
                <span className="text-xs font-mono tabular-nums w-16 text-right">{item.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Domain Status */}
      {domainEntries.length > 0 && (
        <div className="bg-bg-card rounded-xl border border-border p-5">
          <h3 className="font-semibold mb-3 text-sm">Domain Status</h3>
          <div className="flex flex-wrap gap-2">
            {domainEntries.map(([status, count]) => (
              <span key={status} className="bg-amber-50 border border-amber-200 text-xs px-3 py-1 rounded-full">
                {status}: {count}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
