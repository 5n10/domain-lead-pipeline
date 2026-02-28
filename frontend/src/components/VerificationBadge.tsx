import type { BusinessLead } from "../types";

const sourceConfig: Record<string, { label: string; bg: string }> = {
  domain_guess: { label: "DG", bg: "bg-signal-blue" },
  searxng: { label: "SX", bg: "bg-indigo-600" },
  ddg: { label: "DDG", bg: "bg-amber-700" },
  llm: { label: "LLM", bg: "bg-signal-green" },
  google_places: { label: "GP", bg: "bg-signal-green" },
  foursquare: { label: "4SQ", bg: "bg-signal-purple" },
  google_search: { label: "GS", bg: "bg-teal-600" },
};

export default function VerificationBadge({ lead }: { lead: BusinessLead }) {
  const sources = lead.verification_sources ?? [];
  if (sources.length === 0) return <span className="text-text-secondary">-</span>;
  return (
    <span className="flex gap-1 flex-wrap" title={sources.join(", ")}>
      {sources.map((s) => {
        const c = sourceConfig[s] ?? { label: s, bg: "bg-gray-500" };
        return (
          <span key={s} className={`inline-block ${c.bg} text-white rounded px-1.5 py-px text-[0.7rem] font-semibold`}>
            {c.label}
          </span>
        );
      })}
    </span>
  );
}
