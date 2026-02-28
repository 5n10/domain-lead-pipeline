import { FormEvent, useState } from "react";
import { api } from "../api";

type Props = {
  actionLoading: boolean;
  setActionLoading: (v: boolean) => void;
  setStatusMessage: (s: string) => void;
  refresh: () => Promise<void>;
  exportPlatform: string;
  setExportPlatform: (v: string) => void;
  exportMinScore: string;
  setExportMinScore: (v: string) => void;
  exportRequireContact: boolean;
  setExportRequireContact: (v: boolean) => void;
  exportRequireUnhosted: boolean;
  setExportRequireUnhosted: (v: boolean) => void;
  exportRequireDomainQualification: boolean;
  setExportRequireDomainQualification: (v: boolean) => void;
};

function ActionCard({ title, description, cost, onClick, loading, buttonLabel, variant }: {
  title: string;
  description: string;
  cost: string;
  onClick: () => void;
  loading: boolean;
  buttonLabel: string;
  variant?: "green" | "red" | "blue" | "purple" | "teal" | "indigo" | "default";
}) {
  const btnColors: Record<string, string> = {
    green: "bg-signal-green hover:bg-emerald-700",
    red: "bg-signal-red hover:bg-red-700",
    blue: "bg-signal-blue hover:bg-blue-700",
    purple: "bg-signal-purple hover:bg-purple-700",
    teal: "bg-teal-600 hover:bg-teal-700",
    indigo: "bg-indigo-600 hover:bg-indigo-700",
    default: "bg-accent hover:bg-accent-hover",
  };
  return (
    <div className="bg-bg-card rounded-xl border border-border p-4 flex flex-col justify-between">
      <div>
        <h4 className="font-semibold text-sm mb-1">{title}</h4>
        <p className="text-xs text-text-secondary mb-1">{description}</p>
        <span className="text-[0.65rem] font-semibold uppercase tracking-wider text-text-secondary">{cost}</span>
      </div>
      <button
        onClick={onClick}
        disabled={loading}
        className={`mt-3 w-full rounded-lg text-white font-semibold py-2 text-sm transition-colors disabled:opacity-50 ${btnColors[variant ?? "default"]}`}
      >
        {loading ? "Running..." : buttonLabel}
      </button>
    </div>
  );
}

export default function ActionsView(props: Props) {
  const { actionLoading, setActionLoading, setStatusMessage, refresh } = props;

  const [pipelineArea, setPipelineArea] = useState("");
  const [pipelineSyncLimit, setPipelineSyncLimit] = useState("500");
  const [pipelineRdapLimit, setPipelineRdapLimit] = useState("100");
  const [pipelineBusinessScoreLimit, setPipelineBusinessScoreLimit] = useState("2000");
  const [scoreLimit, setScoreLimit] = useState("3000");
  const [scoreForceRescore, setScoreForceRescore] = useState(false);

  async function wrap(label: string, fn: () => Promise<unknown>) {
    setActionLoading(true);
    try {
      const r = await fn() as Record<string, unknown>;
      const msg = r.error ? `${label}: ${r.error}` : `${label}: done`;
      setStatusMessage(msg);
      await refresh();
    } catch (e) { setStatusMessage(`${label} failed: ${(e as Error).message}`); }
    finally { setActionLoading(false); }
  }

  async function runPipeline(e: FormEvent) {
    e.preventDefault();
    setActionLoading(true);
    try {
      await api.runPipeline({
        area: pipelineArea || null,
        categories: "all",
        sync_limit: Number(pipelineSyncLimit) || null,
        rdap_limit: Number(pipelineRdapLimit) || null,
        email_limit: null,
        score_limit: null,
        business_score_limit: Number(pipelineBusinessScoreLimit) || null,
        business_min_score: Number(props.exportMinScore) || null,
        business_platform: props.exportPlatform,
        business_require_contact: props.exportRequireContact,
        business_require_unhosted_domain: props.exportRequireUnhosted,
        business_require_domain_qualification: props.exportRequireDomainQualification,
      });
      setStatusMessage("Pipeline completed");
      await refresh();
    } catch (e) { setStatusMessage(`Pipeline failed: ${(e as Error).message}`); }
    finally { setActionLoading(false); }
  }

  async function runScoring(e: FormEvent) {
    e.preventDefault();
    setActionLoading(true);
    try {
      const r = await api.scoreBusinesses({ limit: Number(scoreLimit) || null, force_rescore: scoreForceRescore });
      setStatusMessage(`Scored ${r.processed} businesses`);
      await refresh();
    } catch (e) { setStatusMessage(`Scoring failed: ${(e as Error).message}`); }
    finally { setActionLoading(false); }
  }

  async function runExport(e: FormEvent) {
    e.preventDefault();
    setActionLoading(true);
    try {
      const r = await api.exportBusinesses({
        platform: props.exportPlatform,
        min_score: Number(props.exportMinScore),
        require_contact: props.exportRequireContact,
        require_unhosted_domain: props.exportRequireUnhosted,
        require_domain_qualification: props.exportRequireDomainQualification,
      });
      setStatusMessage(r.path ? `Export created: ${r.path}` : "No rows exported");
      await refresh();
    } catch (e) { setStatusMessage(`Export failed: ${(e as Error).message}`); }
    finally { setActionLoading(false); }
  }

  return (
    <div className="space-y-6">
      {/* Verification Tools */}
      <div>
        <h3 className="text-sm font-semibold mb-3 text-text-secondary uppercase tracking-wider">Verification Tools</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <ActionCard title="Domain Guess" description="Generate domains from business names and check HTTP HEAD" cost="FREE ~500/min" onClick={() => void wrap("Domain Guess", () => api.domainGuess({ limit: 1000, min_score: 0, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="blue" />
          <ActionCard title="SearXNG Verify" description="Meta-search (DDG+Bing+Brave+Mojeek) for business websites" cost="FREE ~100/min" onClick={() => void wrap("SearXNG Verify", () => api.verifySearXNG({ limit: 200, min_score: 0, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="indigo" />
          <ActionCard title="DDG Search Verify" description="Legacy: Search DuckDuckGo HTML for business websites" cost="FREE ~40/min" onClick={() => void wrap("DDG Verify", () => api.verifyWebsitesDDG({ limit: 300, min_score: 30, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="red" />
          <ActionCard title="Google Search Verify" description="Legacy: Search Google for business websites" cost="FREE ~15/min" onClick={() => void wrap("Google Search", () => api.googleSearchVerify({ limit: 100, min_score: 30, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="teal" />
          <ActionCard title="LLM Verify" description="Verify websites via OpenRouter/Gemini/Groq" cost="API KEY required" onClick={() => void wrap("LLM Verify", () => api.verifyWebsitesLLM({ limit: 100, min_score: 30, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="green" />
          <ActionCard title="Google Places Verify" description="Check Google Places for business websites" cost="API KEY required" onClick={() => void wrap("Google Places Verify", () => api.verifyWebsites({ limit: 200, min_score: 30, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="red" />
          <ActionCard title="Foursquare Verify" description="Verify websites via Foursquare API" cost="API KEY required" onClick={() => void wrap("4SQ Verify", () => api.verifyWebsitesFoursquare({ limit: 200, min_score: 30, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="purple" />
          <ActionCard title="Domain Validation (RDAP)" description="Sync email domains and run RDAP checks" cost="FREE" onClick={() => void wrap("RDAP", () => api.validateDomains({ rdap_limit: 200, rescore: true }))} loading={actionLoading} buttonLabel="Run" variant="default" />
        </div>
      </div>

      {/* Enrichment Tools */}
      <div>
        <h3 className="text-sm font-semibold mb-3 text-text-secondary uppercase tracking-wider">Enrichment Tools</h3>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <ActionCard title="Google Places Enrich" description="Add phone numbers from Google Places" cost="API KEY required" onClick={() => void wrap("GP Enrich", () => api.enrichGooglePlaces({ limit: 200, priority: "no_contacts", rescore: true }))} loading={actionLoading} buttonLabel="Enrich" variant="green" />
          <ActionCard title="Foursquare Enrich" description="Add phone numbers from Foursquare" cost="API KEY required" onClick={() => void wrap("4SQ Enrich", () => api.enrichFoursquare({ limit: 200, priority: "no_contacts", rescore: true }))} loading={actionLoading} buttonLabel="Enrich" variant="purple" />
          <ActionCard title="Hunter.io Emails" description="Find email addresses for lead businesses" cost="API KEY required" onClick={() => void wrap("Hunter", () => api.hunterEnrich({ limit: 25 }))} loading={actionLoading} buttonLabel="Enrich" variant="default" />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Pipeline */}
        <form onSubmit={(e) => void runPipeline(e)} className="bg-bg-card rounded-xl border border-border p-5 space-y-3">
          <h3 className="font-semibold text-sm">Run Full Pipeline</h3>
          <label className="block text-xs text-text-secondary">
            Area
            <input className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={pipelineArea} onChange={(e) => setPipelineArea(e.target.value)} placeholder="e.g. Toronto, Canada" />
          </label>
          <div className="grid grid-cols-3 gap-2">
            <label className="text-xs text-text-secondary">Sync<input type="number" className="mt-1 w-full rounded-lg border border-border px-2 py-1 text-sm font-mono" value={pipelineSyncLimit} onChange={(e) => setPipelineSyncLimit(e.target.value)} /></label>
            <label className="text-xs text-text-secondary">RDAP<input type="number" className="mt-1 w-full rounded-lg border border-border px-2 py-1 text-sm font-mono" value={pipelineRdapLimit} onChange={(e) => setPipelineRdapLimit(e.target.value)} /></label>
            <label className="text-xs text-text-secondary">Score<input type="number" className="mt-1 w-full rounded-lg border border-border px-2 py-1 text-sm font-mono" value={pipelineBusinessScoreLimit} onChange={(e) => setPipelineBusinessScoreLimit(e.target.value)} /></label>
          </div>
          <button type="submit" disabled={actionLoading} className="w-full rounded-lg bg-accent text-white font-semibold py-2 text-sm hover:bg-accent-hover disabled:opacity-50">{actionLoading ? "Running..." : "Run Pipeline"}</button>
        </form>

        {/* Scoring */}
        <form onSubmit={(e) => void runScoring(e)} className="bg-bg-card rounded-xl border border-border p-5 space-y-3">
          <h3 className="font-semibold text-sm">Score Businesses</h3>
          <label className="block text-xs text-text-secondary">
            Limit
            <input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={scoreLimit} onChange={(e) => setScoreLimit(e.target.value)} />
          </label>
          <label className="flex items-center gap-2 text-xs text-text-secondary cursor-pointer">
            <input type="checkbox" className="rounded" checked={scoreForceRescore} onChange={(e) => setScoreForceRescore(e.target.checked)} />
            Force rescore
          </label>
          <button type="submit" disabled={actionLoading} className="w-full rounded-lg bg-accent text-white font-semibold py-2 text-sm hover:bg-accent-hover disabled:opacity-50">{actionLoading ? "Scoring..." : "Score Now"}</button>
        </form>

        {/* Export */}
        <form onSubmit={(e) => void runExport(e)} className="bg-bg-card rounded-xl border border-border p-5 space-y-3">
          <h3 className="font-semibold text-sm">Export Businesses</h3>
          <label className="block text-xs text-text-secondary">
            Platform
            <input className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={props.exportPlatform} onChange={(e) => props.setExportPlatform(e.target.value)} />
          </label>
          <label className="block text-xs text-text-secondary">
            Min Score
            <input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={props.exportMinScore} onChange={(e) => props.setExportMinScore(e.target.value)} />
          </label>
          <div className="space-y-1">
            {[
              { val: props.exportRequireContact, set: props.setExportRequireContact, label: "Require contact" },
              { val: props.exportRequireUnhosted, set: props.setExportRequireUnhosted, label: "Require unhosted domain" },
              { val: props.exportRequireDomainQualification, set: props.setExportRequireDomainQualification, label: "Require domain qualification" },
            ].map(({ val, set, label }) => (
              <label key={label} className="flex items-center gap-2 text-xs text-text-secondary cursor-pointer">
                <input type="checkbox" className="rounded" checked={val} onChange={(e) => set(e.target.checked)} />{label}
              </label>
            ))}
          </div>
          <button type="submit" disabled={actionLoading} className="w-full rounded-lg bg-accent text-white font-semibold py-2 text-sm hover:bg-accent-hover disabled:opacity-50">{actionLoading ? "Exporting..." : "Export Now"}</button>
        </form>
      </div>

      {/* Utility */}
      <div>
        <h3 className="text-sm font-semibold mb-3 text-text-secondary uppercase tracking-wider">Utilities</h3>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <ActionCard title="Export to Sheets" description="Export leads to Google Sheets" cost="Service account required" onClick={() => void wrap("Sheets", () => api.exportGoogleSheets({ min_score: Number(props.exportMinScore) || null, require_contact: props.exportRequireContact }))} loading={actionLoading} buttonLabel="Export" variant="green" />
          <ActionCard title="Test Notification" description="Send a test push notification" cost="NTFY_TOPIC required" onClick={() => void wrap("Notification", () => api.testNotification({}))} loading={actionLoading} buttonLabel="Send" variant="default" />
          <ActionCard title="Reset DDG Data" description="Clear broken DDG verification data and rescore all" cost="One-time fix" onClick={() => void wrap("Reset DDG", () => api.resetDDGVerification())} loading={actionLoading} buttonLabel="Reset" variant="red" />
        </div>
      </div>
    </div>
  );
}
