import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import DuplicatesView from "../components/DuplicatesView";

// Mock global fetch since DuplicatesView uses fetch directly (not api helper)
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

vi.mock("../api", () => ({
  api: {
    baseUrl: "http://127.0.0.1:8000",
  },
}));

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

const mockDuplicatesResponse = {
  total: 2,
  returned: 2,
  clusters: [
    {
      primary: {
        id: "prim-1",
        name: "Best Pizza",
        category: "food",
        address: "100 Queen St",
        lead_score: 90,
      },
      duplicates: [
        {
          id: "dup-1a",
          name: "Best Pizza Inc",
          category: "food",
          address: "100 Queen Street",
          lead_score: 45,
          reason: "name similarity",
        },
      ],
    },
    {
      primary: {
        id: "prim-2",
        name: "City Plumbing",
        category: "plumbing",
        address: null,
        lead_score: null,
      },
      duplicates: [
        {
          id: "dup-2a",
          name: "City Plumbing LLC",
          category: "plumbing",
          address: null,
          lead_score: 30,
          reason: "phone match",
        },
        {
          id: "dup-2b",
          name: null,
          category: null,
          address: null,
          lead_score: null,
        },
      ],
    },
  ],
};

function setupFetchSuccess() {
  mockFetch.mockResolvedValue({
    ok: true,
    json: () => Promise.resolve(mockDuplicatesResponse),
  });
}

function setupFetchError() {
  mockFetch.mockRejectedValue(new Error("Network error"));
}

function setupFetchEmpty() {
  mockFetch.mockResolvedValue({
    ok: true,
    json: () =>
      Promise.resolve({ total: 0, returned: 0, clusters: [] }),
  });
}

describe("DuplicatesView", () => {
  it("renders loading state while fetching", () => {
    mockFetch.mockReturnValue(new Promise(() => {}));
    renderWithClient(<DuplicatesView />);
    expect(
      screen.getByText("Loading duplicate clusters...")
    ).toBeInTheDocument();
  });

  it("renders error state when fetch fails", async () => {
    setupFetchError();
    renderWithClient(<DuplicatesView />);
    expect(
      await screen.findByText("Error loading duplicates")
    ).toBeInTheDocument();
  });

  it("renders empty state when no clusters exist", async () => {
    setupFetchEmpty();
    renderWithClient(<DuplicatesView />);
    expect(
      await screen.findByText("No duplicate clusters found.")
    ).toBeInTheDocument();
  });

  it("renders heading with total duplicate count", async () => {
    setupFetchSuccess();
    renderWithClient(<DuplicatesView />);
    expect(
      await screen.findByText("Duplicate Clusters (2 duplicates)")
    ).toBeInTheDocument();
  });

  it("renders primary business entries with PRIMARY badge", async () => {
    setupFetchSuccess();
    renderWithClient(<DuplicatesView />);

    await screen.findByText("Best Pizza");
    const primaryBadges = screen.getAllByText("PRIMARY");
    expect(primaryBadges).toHaveLength(2);

    expect(screen.getByText("Best Pizza")).toBeInTheDocument();
    expect(screen.getByText("City Plumbing")).toBeInTheDocument();
    expect(screen.getByText("Score: 90")).toBeInTheDocument();
    expect(screen.getByText("100 Queen St")).toBeInTheDocument();
  });

  it("renders duplicate entries with DUP badge and reasons", async () => {
    setupFetchSuccess();
    renderWithClient(<DuplicatesView />);

    await screen.findByText("Best Pizza Inc");
    const dupBadges = screen.getAllByText("DUP");
    expect(dupBadges).toHaveLength(3);

    expect(screen.getByText("Best Pizza Inc")).toBeInTheDocument();
    expect(screen.getByText("Score: 45")).toBeInTheDocument();
    expect(screen.getByText("name similarity")).toBeInTheDocument();

    expect(screen.getByText("City Plumbing LLC")).toBeInTheDocument();
    expect(screen.getByText("Score: 30")).toBeInTheDocument();
    expect(screen.getByText("phone match")).toBeInTheDocument();
  });

  it("renders Dismiss buttons for each duplicate", async () => {
    setupFetchSuccess();
    renderWithClient(<DuplicatesView />);

    await screen.findByText("Best Pizza Inc");
    const dismissButtons = screen.getAllByText("Dismiss");
    // 3 duplicates total across all clusters
    expect(dismissButtons).toHaveLength(3);
  });

  it("calls dismiss API when Dismiss button is clicked", async () => {
    setupFetchSuccess();
    renderWithClient(<DuplicatesView />);

    await screen.findByText("Best Pizza Inc");

    // Reset mock to track dismiss call
    mockFetch.mockClear();
    mockFetch.mockResolvedValue({ ok: true, json: () => Promise.resolve({}) });

    const dismissButtons = screen.getAllByText("Dismiss");
    fireEvent.click(dismissButtons[0]);

    // The dismiss call should be made to the correct URL
    expect(mockFetch).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/actions/dismiss-duplicate?business_id=dup-1a",
      expect.objectContaining({ method: "POST" })
    );
  });
});
