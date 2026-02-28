import type { BusinessLead, BusinessLeadResponse, Metrics } from "../types";
import ConfidenceBadge from "./ConfidenceBadge";
import VerificationBadge from "./VerificationBadge";

export type LeadFilters = {
  minScore: string;
  category: string;
  city: string;
  minConfidence: string;
  requireContact: boolean;
  requireUnhostedDomain: boolean;
  requireDomainQualification: boolean;
  requireNoWebsite: boolean;
  excludeHostedEmailDomain: boolean;
  onlyUnexported: boolean;
  onlyVerified: boolean;
  limit: string;
};

export const defaultFilters: LeadFilters = {
  minScore: "",
  category: "all",
  city: "",
  minConfidence: "",
  requireContact: false,
  requireUnhostedDomain: false,
  requireDomainQualification: false,
  requireNoWebsite: true,
  excludeHostedEmailDomain: true,
  onlyUnexported: false,
  onlyVerified: false,
  limit: "200",
};

type Props = {
  leads: BusinessLeadResponse | null;
  metrics: Metrics | null;
  filters: LeadFilters;
  categories: string[];
  cities: string[];
  loading: boolean;
  onFiltersChange: (f: LeadFilters) => void;
  onApply: () => void;
};

function ScoreBar({ score }: { score: number | null }) {
  if (score == null) return <span className="text-text-secondary">-</span>;
  const pct = Math.min(score, 100);
  const color = score >= 60 ? "bg-signal-green" : score >= 40 ? "bg-signal-amber" : score >= 20 ? "bg-orange-500" : "bg-signal-red";
  return (
    <div className="flex items-center gap-2">
      <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-mono tabular-nums">{score}</span>
    </div>
  );
}

function LeadRow({ lead }: { lead: BusinessLead }) {
  const domains = lead.verified_unhosted_domains.join(", ") || lead.unregistered_domains.join(", ") || lead.domains.join(", ");
  return (
    <tr className="hover:bg-amber-50/50 transition-colors">
      <td className="px-3 py-2.5 text-sm max-w-[200px] truncate" title={lead.name ?? ""}>{lead.name || "-"}</td>
      <td className="px-3 py-2.5 text-xs text-text-secondary">{lead.category || "-"}</td>
      <td className="px-3 py-2.5 text-xs">{lead.city || "-"}</td>
      <td className="px-3 py-2.5"><ScoreBar score={lead.lead_score} /></td>
      <td className="px-3 py-2.5"><ConfidenceBadge confidence={lead.verification_confidence ?? "unverified"} /></td>
      <td className="px-3 py-2.5"><VerificationBadge lead={lead} /></td>
      <td className="px-3 py-2.5 text-xs font-mono max-w-[180px] truncate" title={lead.business_emails.join(", ") || lead.emails.join(", ")}>
        {lead.business_emails.join(", ") || lead.emails.join(", ") || "-"}
      </td>
      <td className="px-3 py-2.5 text-xs font-mono">{lead.phones.join(", ") || "-"}</td>
      <td className="px-3 py-2.5 text-xs font-mono max-w-[150px] truncate" title={domains}>{domains || "-"}</td>
      <td className="px-3 py-2.5 text-xs">{lead.exported ? <span className="text-signal-green">Yes</span> : <span className="text-text-secondary">No</span>}</td>
    </tr>
  );
}

export default function LeadsView({ leads, metrics, filters, categories, cities, loading, onFiltersChange, onApply }: Props) {
  const setF = (patch: Partial<LeadFilters>) => onFiltersChange({ ...filters, ...patch });

  return (
    <div className="space-y-4">
      {/* Filters */}
      <form
        className="bg-bg-card rounded-xl border border-border p-4"
        onSubmit={(e) => { e.preventDefault(); onApply(); }}
      >
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mb-3">
          <label className="text-xs text-text-secondary">
            Min Score
            <input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={filters.minScore} onChange={(e) => setF({ minScore: e.target.value })} />
          </label>
          <label className="text-xs text-text-secondary">
            Category
            <select className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={filters.category} onChange={(e) => setF({ category: e.target.value })}>
              <option value="all">All</option>
              {categories.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
          <label className="text-xs text-text-secondary">
            City
            <input className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={filters.city} list="city-opts" onChange={(e) => setF({ city: e.target.value })} />
            <datalist id="city-opts">{cities.map((c) => <option key={c} value={c} />)}</datalist>
          </label>
          <label className="text-xs text-text-secondary">
            Min Confidence
            <select className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm" value={filters.minConfidence} onChange={(e) => setF({ minConfidence: e.target.value })}>
              <option value="">All</option>
              <option value="high">High</option>
              <option value="medium">Medium+</option>
              <option value="low">Low+</option>
            </select>
          </label>
          <label className="text-xs text-text-secondary">
            Limit
            <input type="number" className="mt-1 w-full rounded-lg border border-border px-3 py-1.5 text-sm font-mono" value={filters.limit} onChange={(e) => setF({ limit: e.target.value })} />
          </label>
          <div className="flex items-end">
            <button type="submit" disabled={loading} className="w-full rounded-lg bg-accent text-white font-semibold py-1.5 px-4 text-sm hover:bg-accent-hover disabled:opacity-50 transition-colors">
              Apply
            </button>
          </div>
        </div>
        <div className="flex flex-wrap gap-x-5 gap-y-1">
          {[
            { key: "requireContact" as const, label: "Require contact" },
            { key: "requireNoWebsite" as const, label: "No website only" },
            { key: "excludeHostedEmailDomain" as const, label: "Exclude hosted email domains" },
            { key: "requireUnhostedDomain" as const, label: "Require unhosted domain" },
            { key: "requireDomainQualification" as const, label: "Require domain qualification" },
            { key: "onlyUnexported" as const, label: "Only unexported" },
            { key: "onlyVerified" as const, label: "Only verified" },
          ].map(({ key, label }) => (
            <label key={key} className="flex items-center gap-1.5 text-xs text-text-secondary cursor-pointer">
              <input type="checkbox" className="rounded border-border" checked={filters[key]} onChange={(e) => setF({ [key]: e.target.checked })} />
              {label}
            </label>
          ))}
        </div>
      </form>

      {/* Summary */}
      <p className="text-sm text-text-secondary px-1">
        Showing <span className="font-semibold text-text-primary">{leads?.returned ?? 0}</span> of{" "}
        <span className="font-semibold text-text-primary">{(leads?.total_candidates ?? 0).toLocaleString()}</span> candidates
        {metrics && (
          <span> ({metrics.businesses.total.toLocaleString()} total businesses)</span>
        )}
      </p>

      {/* Table */}
      <div className="bg-bg-card rounded-xl border border-border overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-amber-50/80">
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Name</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Category</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">City</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Score</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Confidence</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Verified</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Email</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Phone</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Domain</th>
                <th className="px-3 py-2.5 text-xs font-semibold text-text-secondary">Exported</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {leads?.items.map((lead) => <LeadRow key={lead.id} lead={lead} />)}
            </tbody>
          </table>
          {(!leads || leads.items.length === 0) && (
            <p className="text-center text-text-secondary py-8 text-sm">No leads found. Try adjusting filters.</p>
          )}
        </div>
      </div>
    </div>
  );
}
