import type { AutomationStatus, BusinessLeadResponse, ExportFile, JobRun, Metrics } from "./types";

const API_BASE =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ??
  (typeof window !== "undefined" && window.location.port === "8000"
    ? window.location.origin
    : "http://127.0.0.1:8000");
const MUTATION_API_KEY = (import.meta.env.VITE_MUTATION_API_KEY as string | undefined) ?? "";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const useMutationKey = Boolean(init?.method && init.method.toUpperCase() !== "GET" && MUTATION_API_KEY);
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(useMutationKey ? { "X-API-Key": MUTATION_API_KEY } : {}),
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }

  return (await response.json()) as T;
}

export const api = {
  baseUrl: API_BASE,
  metrics: () => request<Metrics>("/api/metrics"),
  jobs: (limit = 50) => request<JobRun[]>(`/api/jobs?limit=${limit}`),
  categories: () => request<string[]>("/api/leads/business/categories"),
  cities: () => request<string[]>("/api/leads/business/cities?limit=500"),
  businessLeads: (query: URLSearchParams) =>
    request<BusinessLeadResponse>(`/api/leads/business?${query.toString()}`),
  exportFiles: () => request<ExportFile[]>("/api/exports/files"),
  runPipeline: (payload: Record<string, unknown>) =>
    request<Record<string, unknown>>("/api/actions/pipeline-run", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  scoreBusinesses: (payload: Record<string, unknown>) =>
    request<{ processed: number }>("/api/actions/business-score", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  exportBusinesses: (payload: Record<string, unknown>) =>
    request<{ path: string | null }>("/api/actions/business-export", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  automationStatus: () => request<AutomationStatus>("/api/automation/status"),
  automationStart: (payload: Record<string, unknown>) =>
    request<AutomationStatus>("/api/automation/start", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  automationStop: () =>
    request<AutomationStatus>("/api/automation/stop", {
      method: "POST"
    }),
  automationRunNow: () =>
    request<Record<string, unknown>>("/api/automation/run-now", {
      method: "POST"
    }),
  automationDailyTargetNow: () =>
    request<Record<string, unknown>>("/api/automation/daily-target-now", {
      method: "POST"
    }),
  automationUpdateSettings: (payload: Record<string, unknown>) =>
    request<AutomationStatus>("/api/automation/settings", {
      method: "POST",
      body: JSON.stringify(payload)
    })
};
