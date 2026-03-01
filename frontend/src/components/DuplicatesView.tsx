import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";

type DuplicateBusiness = {
  id: string;
  name: string | null;
  category: string | null;
  address: string | null;
  lead_score: number | null;
  reason?: string;
};

type DuplicateCluster = {
  primary: DuplicateBusiness;
  duplicates: DuplicateBusiness[];
};

type DuplicatesResponse = {
  total: number;
  returned: number;
  clusters: DuplicateCluster[];
};

export default function DuplicatesView() {
  const queryClient = useQueryClient();
  const { data, isLoading, error } = useQuery({
    queryKey: ["duplicates"],
    queryFn: () =>
      fetch(`${api.baseUrl}/api/duplicates?limit=100&offset=0`).then(
        (r) => r.json() as Promise<DuplicatesResponse>
      ),
    refetchInterval: 60_000,
  });

  async function handleDismiss(businessId: string) {
    try {
      await fetch(
        `${api.baseUrl}/api/actions/dismiss-duplicate?business_id=${businessId}`,
        { method: "POST", headers: { "Content-Type": "application/json" } }
      );
      void queryClient.invalidateQueries({ queryKey: ["duplicates"] });
    } catch {
      // silently fail
    }
  }

  if (isLoading)
    return (
      <p className="text-gray-400 p-4">Loading duplicate clusters...</p>
    );
  if (error)
    return <p className="text-red-400 p-4">Error loading duplicates</p>;
  if (!data || data.clusters.length === 0)
    return (
      <p className="text-gray-500 p-4">No duplicate clusters found.</p>
    );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-200">
          Duplicate Clusters ({data.total} duplicates)
        </h2>
      </div>

      <div className="space-y-3">
        {data.clusters.map((cluster) => (
          <div
            key={cluster.primary.id}
            className="border border-gray-700 rounded-lg p-4"
          >
            {/* Primary */}
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xs font-medium bg-green-900/50 text-green-300 px-2 py-0.5 rounded">
                PRIMARY
              </span>
              <span className="text-gray-200 font-medium">
                {cluster.primary.name || "-"}
              </span>
              <span className="text-gray-500 text-sm">
                {cluster.primary.category || ""}
              </span>
              {cluster.primary.lead_score != null && (
                <span className="text-xs text-gray-400">
                  Score: {cluster.primary.lead_score}
                </span>
              )}
            </div>
            {cluster.primary.address && (
              <p className="text-xs text-gray-500 mb-3 pl-16">
                {cluster.primary.address}
              </p>
            )}

            {/* Duplicates */}
            {cluster.duplicates.map((dup) => (
              <div
                key={dup.id}
                className="flex items-center gap-2 py-1.5 pl-4 border-t border-gray-800"
              >
                <span className="text-xs font-medium bg-red-900/50 text-red-300 px-2 py-0.5 rounded">
                  DUP
                </span>
                <span className="text-gray-300">{dup.name || "-"}</span>
                <span className="text-gray-500 text-sm">
                  {dup.category || ""}
                </span>
                {dup.lead_score != null && (
                  <span className="text-xs text-gray-400">
                    Score: {dup.lead_score}
                  </span>
                )}
                {dup.reason && (
                  <span className="text-xs text-yellow-500/70">{dup.reason}</span>
                )}
                <div className="flex-1" />
                <button
                  onClick={() => void handleDismiss(dup.id)}
                  className="text-xs text-gray-500 hover:text-gray-300 px-2 py-1 rounded border border-gray-700 hover:border-gray-500"
                >
                  Dismiss
                </button>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}
