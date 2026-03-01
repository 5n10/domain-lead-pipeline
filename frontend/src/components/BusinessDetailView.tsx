import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

type Props = {
  businessId: string;
  onBack: () => void;
};

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border border-gray-700 rounded-lg p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-2">{title}</h3>
      {children}
    </div>
  );
}

function Badge({ label, color }: { label: string; color: string }) {
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {label}
    </span>
  );
}

const CONFIDENCE_COLORS: Record<string, string> = {
  high: "bg-green-900/50 text-green-300",
  medium: "bg-yellow-900/50 text-yellow-300",
  low: "bg-orange-900/50 text-orange-300",
  unverified: "bg-gray-700 text-gray-400",
};

export default function BusinessDetailView({ businessId, onBack }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["business-detail", businessId],
    queryFn: () => api.businessDetail(businessId),
  });

  if (isLoading) return <p className="text-gray-400 p-4">Loading business detail...</p>;
  if (error) return <p className="text-red-400 p-4">Error loading business detail</p>;
  if (!data) return <p className="text-gray-500 p-4">Business not found.</p>;

  const biz = data as Record<string, unknown>;
  const timeline = (biz.verification_timeline as Array<Record<string, unknown>>) || [];
  const scoreReasons = (biz.score_reasons as Record<string, unknown>) || {};
  const exports = (biz.exports as Array<Record<string, unknown>>) || [];
  const confidence = (biz.verification_confidence as string) || "unverified";

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button
          onClick={onBack}
          className="text-gray-400 hover:text-white text-sm px-2 py-1 rounded border border-gray-700 hover:border-gray-500"
        >
          Back
        </button>
        <h2 className="text-lg font-semibold text-gray-200">
          {(biz.name as string) || "Unnamed Business"}
        </h2>
        <Badge label={confidence} color={CONFIDENCE_COLORS[confidence] || CONFIDENCE_COLORS.unverified} />
        {biz.lead_score != null && (
          <span className="text-sm text-gray-400">Score: {biz.lead_score as number}</span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Basic Info */}
        <Section title="Basic Info">
          <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
            <dt className="text-gray-500">Category</dt>
            <dd className="text-gray-300">{(biz.category as string) || "-"}</dd>
            <dt className="text-gray-500">Address</dt>
            <dd className="text-gray-300">{(biz.address as string) || "-"}</dd>
            <dt className="text-gray-500">City</dt>
            <dd className="text-gray-300">{(biz.city as string) || "-"}</dd>
            <dt className="text-gray-500">Country</dt>
            <dd className="text-gray-300">{(biz.country as string) || "-"}</dd>
            <dt className="text-gray-500">Website</dt>
            <dd className="text-gray-300">{(biz.website_url as string) || "-"}</dd>
            <dt className="text-gray-500">Source</dt>
            <dd className="text-gray-300">{biz.source as string}/{biz.source_id as string}</dd>
            <dt className="text-gray-500">Created</dt>
            <dd className="text-gray-300">{biz.created_at ? new Date(biz.created_at as string).toLocaleString() : "-"}</dd>
            <dt className="text-gray-500">Scored</dt>
            <dd className="text-gray-300">{biz.scored_at ? new Date(biz.scored_at as string).toLocaleString() : "-"}</dd>
          </dl>
        </Section>

        {/* Score Breakdown */}
        <Section title="Score Breakdown">
          {Object.keys(scoreReasons).length > 0 ? (
            <ul className="text-sm space-y-1">
              {Object.entries(scoreReasons).map(([key, value]) => (
                <li key={key} className="flex justify-between">
                  <span className="text-gray-400">{key.replace(/_/g, " ")}</span>
                  <span className={`font-mono ${(value as number) > 0 ? "text-green-400" : "text-red-400"}`}>
                    {(value as number) > 0 ? "+" : ""}{value as number}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-gray-500 text-sm">No score breakdown available</p>
          )}
        </Section>

        {/* Contacts */}
        <Section title="Contacts">
          <div className="text-sm space-y-2">
            {((biz.business_emails as string[]) || []).length > 0 && (
              <div>
                <span className="text-gray-500">Business Emails: </span>
                <span className="text-gray-300">{(biz.business_emails as string[]).join(", ")}</span>
              </div>
            )}
            {((biz.free_emails as string[]) || []).length > 0 && (
              <div>
                <span className="text-gray-500">Free Emails: </span>
                <span className="text-gray-300">{(biz.free_emails as string[]).join(", ")}</span>
              </div>
            )}
            {((biz.phones as string[]) || []).length > 0 && (
              <div>
                <span className="text-gray-500">Phones: </span>
                <span className="text-gray-300">{(biz.phones as string[]).join(", ")}</span>
              </div>
            )}
            {((biz.business_emails as string[]) || []).length === 0 &&
              ((biz.phones as string[]) || []).length === 0 && (
              <p className="text-gray-500">No contacts found</p>
            )}
          </div>
        </Section>

        {/* Domains */}
        <Section title="Domains">
          <div className="text-sm space-y-1">
            {((biz.verified_unhosted_domains as string[]) || []).map((d) => (
              <div key={d} className="flex items-center gap-2">
                <Badge label="unhosted" color="bg-green-900/50 text-green-300" />
                <span className="text-gray-300">{d}</span>
              </div>
            ))}
            {((biz.hosted_domains as string[]) || []).map((d) => (
              <div key={d} className="flex items-center gap-2">
                <Badge label="hosted" color="bg-red-900/50 text-red-300" />
                <span className="text-gray-300">{d}</span>
              </div>
            ))}
            {((biz.parked_domains as string[]) || []).map((d) => (
              <div key={d} className="flex items-center gap-2">
                <Badge label="parked" color="bg-yellow-900/50 text-yellow-300" />
                <span className="text-gray-300">{d}</span>
              </div>
            ))}
            {((biz.unknown_domains as string[]) || []).map((d) => (
              <div key={d} className="flex items-center gap-2">
                <Badge label="unknown" color="bg-gray-700 text-gray-400" />
                <span className="text-gray-300">{d}</span>
              </div>
            ))}
            {((biz.domains as string[]) || []).length === 0 && (
              <p className="text-gray-500">No domains linked</p>
            )}
          </div>
        </Section>

        {/* Verification Timeline */}
        <Section title="Verification Timeline">
          {timeline.length > 0 ? (
            <div className="space-y-2">
              {timeline.map((entry, i) => (
                <div key={i} className="flex items-center gap-3 text-sm">
                  <Badge
                    label={entry.source as string}
                    color={entry.result === "has_website" ? "bg-green-900/50 text-green-300" :
                           entry.result === "no_website" ? "bg-red-900/50 text-red-300" :
                           "bg-gray-700 text-gray-400"}
                  />
                  <span className="text-gray-400">{entry.result as string || "checked"}</span>
                  {entry.website_found && (
                    <span className="text-green-400 text-xs">{entry.website_found as string}</span>
                  )}
                  <span className="text-gray-600 text-xs ml-auto">
                    {entry.timestamp ? new Date(entry.timestamp as string).toLocaleString() : ""}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-gray-500 text-sm">Not yet verified by any source</p>
          )}
        </Section>

        {/* Export History */}
        <Section title="Export History">
          {exports.length > 0 ? (
            <ul className="text-sm space-y-1">
              {exports.map((exp, i) => (
                <li key={i} className="text-gray-300">
                  {exp.platform as string} — {exp.exported_at ? new Date(exp.exported_at as string).toLocaleString() : "pending"}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-gray-500 text-sm">Not exported yet</p>
          )}
        </Section>
      </div>
    </div>
  );
}
