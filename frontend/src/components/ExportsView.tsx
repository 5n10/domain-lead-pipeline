import type { ExportFile } from "../types";
import { api } from "../api";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(ts: number): string {
  return new Date(ts * 1000).toLocaleString();
}

export default function ExportsView({ files }: { files: ExportFile[] }) {
  return (
    <div className="space-y-4">
      {files.length === 0 ? (
        <p className="text-text-secondary text-sm p-6">No export files found.</p>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {files.map((file) => (
            <a
              key={file.name}
              href={`${api.baseUrl}/api/exports/files/${encodeURIComponent(file.name)}`}
              target="_blank"
              rel="noreferrer"
              className="block bg-bg-card rounded-xl border border-border p-4 hover:border-accent hover:shadow-md transition-all group"
            >
              <div className="flex items-start gap-3">
                <div className="mt-1 w-8 h-8 bg-amber-100 rounded-lg flex items-center justify-center text-accent text-sm font-bold group-hover:bg-accent group-hover:text-white transition-colors">
                  CSV
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-semibold truncate group-hover:text-accent transition-colors">{file.name}</p>
                  <p className="text-xs text-text-secondary mt-1">{formatBytes(file.size)} &middot; {formatDate(file.modified_at)}</p>
                </div>
              </div>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}
