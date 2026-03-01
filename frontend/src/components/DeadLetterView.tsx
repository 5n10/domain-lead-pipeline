import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import type { DeadLetterBusiness } from "../types";

function formatDate(iso: string | null): string {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString();
}

export default function DeadLetterView() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["dead-letter"],
    queryFn: () => api.deadLetter(200, 0),
    refetchInterval: 30_000,
  });

  if (isLoading) return <p className="text-gray-400 p-4">Loading dead-letter queue...</p>;
  if (error) return <p className="text-red-400 p-4">Error loading dead-letter queue</p>;
  if (!data || data.items.length === 0)
    return <p className="text-gray-500 p-4">No businesses in the dead-letter queue.</p>;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-200">
          Dead-Letter Queue ({data.total})
        </h2>
        <span className="text-sm text-gray-500">
          Businesses that failed verification 3+ times
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left">
          <thead className="text-xs text-gray-400 uppercase border-b border-gray-700">
            <tr>
              <th className="px-3 py-2">Name</th>
              <th className="px-3 py-2">Category</th>
              <th className="px-3 py-2">Retries</th>
              <th className="px-3 py-2">Error</th>
              <th className="px-3 py-2">Error At</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((biz: DeadLetterBusiness) => (
              <tr key={biz.id} className="border-b border-gray-800 hover:bg-gray-800/50">
                <td className="px-3 py-2 text-gray-200">{biz.name || "-"}</td>
                <td className="px-3 py-2 text-gray-400">{biz.category || "-"}</td>
                <td className="px-3 py-2">
                  <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-900/50 text-red-300">
                    {biz.retry_count}
                  </span>
                </td>
                <td className="px-3 py-2 text-red-400 max-w-md truncate" title={biz.error || ""}>
                  {biz.error || "-"}
                </td>
                <td className="px-3 py-2 text-gray-500 whitespace-nowrap">
                  {formatDate(biz.error_at)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
