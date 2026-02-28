import type { JobRun } from "../types";

function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

export default function JobsView({ jobs }: { jobs: JobRun[] }) {
  return (
    <div className="bg-bg-card rounded-xl border border-border overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-amber-50/80">
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Job</th>
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Scope</th>
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Status</th>
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Processed</th>
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Started</th>
              <th className="px-4 py-3 text-xs font-semibold text-text-secondary">Error</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {jobs.map((job) => (
              <tr key={job.id} className={`${job.status === "failed" ? "bg-red-50" : "hover:bg-amber-50/50"} transition-colors`}>
                <td className="px-4 py-2.5 text-sm font-medium">{job.job_name}</td>
                <td className="px-4 py-2.5 text-xs text-text-secondary">{job.scope || "-"}</td>
                <td className="px-4 py-2.5">
                  <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold
                    ${job.status === "completed" ? "bg-signal-green text-white" :
                      job.status === "failed" ? "bg-signal-red text-white" :
                      job.status === "running" ? "bg-signal-amber text-white" :
                      "bg-gray-200 text-gray-700"}`}>
                    {job.status}
                  </span>
                </td>
                <td className="px-4 py-2.5 text-sm font-mono tabular-nums">{job.processed_count}</td>
                <td className="px-4 py-2.5 text-xs text-text-secondary">{formatDate(job.started_at)}</td>
                <td className="px-4 py-2.5 text-xs text-signal-red max-w-[200px] truncate" title={job.error ?? ""}>
                  {job.error ? job.error.substring(0, 80) + (job.error.length > 80 ? "..." : "") : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {jobs.length === 0 && <p className="text-center text-text-secondary py-8 text-sm">No jobs found.</p>}
      </div>
    </div>
  );
}
