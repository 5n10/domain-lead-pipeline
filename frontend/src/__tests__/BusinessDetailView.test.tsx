import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import BusinessDetailView from "../components/BusinessDetailView";

vi.mock("../api", () => ({
  api: {
    businessDetail: vi.fn(),
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

const mockBusiness: Record<string, unknown> = {
  name: "Acme Plumbing",
  category: "plumbing",
  address: "123 Main St",
  city: "Toronto",
  country: "CA",
  website_url: "https://acme.com",
  source: "osm",
  source_id: "node/999",
  lead_score: 82,
  verification_confidence: "high",
  created_at: "2025-06-01T10:00:00Z",
  scored_at: "2025-06-02T12:00:00Z",
  business_emails: ["info@acme.com"],
  free_emails: ["owner@gmail.com"],
  phones: ["+14165551234"],
  verified_unhosted_domains: ["acme.com"],
  hosted_domains: ["acme.wixsite.com"],
  parked_domains: ["acme.net"],
  unknown_domains: ["acme.xyz"],
  domains: ["acme.com", "acme.wixsite.com", "acme.net", "acme.xyz"],
  score_reasons: { has_website: 20, has_email: 10, no_chain: 5 },
  verification_timeline: [
    {
      source: "domain_guess",
      result: "has_website",
      website_found: "acme.com",
      timestamp: "2025-06-01T11:00:00Z",
    },
    {
      source: "searxng",
      result: "no_website",
      website_found: null,
      timestamp: "2025-06-01T12:00:00Z",
    },
  ],
  exports: [
    { platform: "ghl", exported_at: "2025-06-03T08:00:00Z" },
  ],
};

describe("BusinessDetailView", () => {
  it("renders loading state while fetching", () => {
    // Make the query stay in pending state by returning a never-resolving promise
    vi.mocked(api.businessDetail).mockReturnValue(new Promise(() => {}));
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);
    expect(screen.getByText("Loading business detail...")).toBeInTheDocument();
  });

  it("renders error state when query fails", async () => {
    vi.mocked(api.businessDetail).mockRejectedValue(new Error("Server error"));
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);
    expect(await screen.findByText("Error loading business detail")).toBeInTheDocument();
  });

  it("renders business info when data loads", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("Acme Plumbing")).toBeInTheDocument();
    expect(screen.getByText("high")).toBeInTheDocument();
    expect(screen.getByText("Score: 82")).toBeInTheDocument();
    expect(screen.getByText("plumbing")).toBeInTheDocument();
    expect(screen.getByText("123 Main St")).toBeInTheDocument();
    expect(screen.getByText("Toronto")).toBeInTheDocument();
    expect(screen.getByText("CA")).toBeInTheDocument();
    expect(screen.getByText("https://acme.com")).toBeInTheDocument();
    expect(screen.getByText("osm/node/999")).toBeInTheDocument();
  });

  it("calls onBack when the Back button is clicked", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    const backButton = await screen.findByText("Back");
    fireEvent.click(backButton);
    expect(onBack).toHaveBeenCalledTimes(1);
  });

  it("renders contacts section with emails and phones", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("info@acme.com")).toBeInTheDocument();
    expect(screen.getByText("owner@gmail.com")).toBeInTheDocument();
    expect(screen.getByText("+14165551234")).toBeInTheDocument();
    expect(screen.getByText("Contacts")).toBeInTheDocument();
  });

  it("renders score breakdown with reasons", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("Score Breakdown")).toBeInTheDocument();
    expect(screen.getByText("has website")).toBeInTheDocument();
    expect(screen.getByText("+20")).toBeInTheDocument();
    expect(screen.getByText("has email")).toBeInTheDocument();
    expect(screen.getByText("+10")).toBeInTheDocument();
    expect(screen.getByText("no chain")).toBeInTheDocument();
    expect(screen.getByText("+5")).toBeInTheDocument();
  });

  it("renders verification timeline entries", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("Verification Timeline")).toBeInTheDocument();
    expect(screen.getByText("domain_guess")).toBeInTheDocument();
    expect(screen.getByText("searxng")).toBeInTheDocument();
    expect(screen.getByText("has_website")).toBeInTheDocument();
    expect(screen.getByText("no_website")).toBeInTheDocument();
    // "acme.com" appears in both the Domains section and the timeline website_found
    expect(screen.getAllByText("acme.com").length).toBeGreaterThanOrEqual(1);
  });

  it("renders domain badges with correct labels", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("Domains")).toBeInTheDocument();
    expect(screen.getByText("unhosted")).toBeInTheDocument();
    expect(screen.getByText("hosted")).toBeInTheDocument();
    expect(screen.getByText("parked")).toBeInTheDocument();
    expect(screen.getByText("unknown")).toBeInTheDocument();
  });

  it("renders export history", async () => {
    vi.mocked(api.businessDetail).mockResolvedValue(mockBusiness);
    const onBack = vi.fn();
    renderWithClient(<BusinessDetailView businessId="abc-123" onBack={onBack} />);

    expect(await screen.findByText("Export History")).toBeInTheDocument();
    expect(screen.getByText(/ghl/)).toBeInTheDocument();
  });
});
