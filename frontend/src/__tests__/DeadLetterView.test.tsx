import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import DeadLetterView from "../components/DeadLetterView";
import type { DeadLetterResponse } from "../types";

vi.mock("../api", () => ({
  api: {
    deadLetter: vi.fn(),
  },
}));

import { api } from "../api";

let queryClient: QueryClient;

beforeEach(() => {
  queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  vi.clearAllMocks();
});

function renderWithClient(ui: React.ReactElement) {
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>
  );
}

const mockDeadLetterResponse: DeadLetterResponse = {
  total: 3,
  returned: 3,
  items: [
    {
      id: "dl-1",
      name: "Broken Plumbing Co",
      category: "plumbing",
      retry_count: 5,
      error: "Timeout connecting to verification service",
      error_at: "2025-06-01T14:00:00Z",
    },
    {
      id: "dl-2",
      name: "Ghost Cafe",
      category: "food",
      retry_count: 3,
      error: "DNS resolution failed",
      error_at: "2025-06-02T09:30:00Z",
    },
    {
      id: "dl-3",
      name: null,
      category: null,
      retry_count: 4,
      error: null,
      error_at: null,
    },
  ],
};

describe("DeadLetterView", () => {
  it("renders loading state while fetching", () => {
    vi.mocked(api.deadLetter).mockReturnValue(new Promise(() => {}));
    renderWithClient(<DeadLetterView />);
    expect(screen.getByText("Loading dead-letter queue...")).toBeInTheDocument();
  });

  it("renders error state when query fails", async () => {
    vi.mocked(api.deadLetter).mockRejectedValue(new Error("Server error"));
    renderWithClient(<DeadLetterView />);
    expect(
      await screen.findByText("Error loading dead-letter queue")
    ).toBeInTheDocument();
  });

  it("renders empty state when no items", async () => {
    vi.mocked(api.deadLetter).mockResolvedValue({
      total: 0,
      returned: 0,
      items: [],
    });
    renderWithClient(<DeadLetterView />);
    expect(
      await screen.findByText("No businesses in the dead-letter queue.")
    ).toBeInTheDocument();
  });

  it("renders dead-letter queue heading with total count", async () => {
    vi.mocked(api.deadLetter).mockResolvedValue(mockDeadLetterResponse);
    renderWithClient(<DeadLetterView />);
    expect(
      await screen.findByText("Dead-Letter Queue (3)")
    ).toBeInTheDocument();
    expect(
      screen.getByText("Businesses that failed verification 3+ times")
    ).toBeInTheDocument();
  });

  it("renders table headers", async () => {
    vi.mocked(api.deadLetter).mockResolvedValue(mockDeadLetterResponse);
    renderWithClient(<DeadLetterView />);
    await screen.findByText("Name");
    expect(screen.getByText("Name")).toBeInTheDocument();
    expect(screen.getByText("Category")).toBeInTheDocument();
    expect(screen.getByText("Retries")).toBeInTheDocument();
    expect(screen.getByText("Error")).toBeInTheDocument();
    expect(screen.getByText("Error At")).toBeInTheDocument();
  });

  it("renders dead-letter items with business data", async () => {
    vi.mocked(api.deadLetter).mockResolvedValue(mockDeadLetterResponse);
    renderWithClient(<DeadLetterView />);

    expect(await screen.findByText("Broken Plumbing Co")).toBeInTheDocument();
    expect(screen.getByText("plumbing")).toBeInTheDocument();
    expect(screen.getByText("5")).toBeInTheDocument();
    expect(
      screen.getByText("Timeout connecting to verification service")
    ).toBeInTheDocument();

    expect(screen.getByText("Ghost Cafe")).toBeInTheDocument();
    expect(screen.getByText("food")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    expect(screen.getByText("DNS resolution failed")).toBeInTheDocument();
  });

  it("renders dashes for null name, category, and error", async () => {
    vi.mocked(api.deadLetter).mockResolvedValue(mockDeadLetterResponse);
    renderWithClient(<DeadLetterView />);

    await screen.findByText("Broken Plumbing Co");
    // dl-3 has null name, category, error => all render as "-"
    const dashes = screen.getAllByText("-");
    expect(dashes.length).toBeGreaterThanOrEqual(3);
  });
});
