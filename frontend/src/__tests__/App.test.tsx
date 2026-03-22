import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

// Mock all child views to isolate App behavior
vi.mock("../components/DashboardView", () => ({
  default: () => <div data-testid="dashboard-view">Dashboard</div>,
}));
vi.mock("../components/LeadsView", () => ({
  default: () => <div data-testid="leads-view">Leads</div>,
  defaultFilters: {
    minScore: "", category: "all", city: "", minConfidence: "",
    requireContact: false, requireUnhostedDomain: false,
    requireDomainQualification: false, requireNoWebsite: false,
    excludeHostedEmailDomain: false, onlyUnexported: false,
    onlyVerified: false, limit: "200",
  },
}));
vi.mock("../components/ActionsView", () => ({
  default: () => <div data-testid="actions-view">Actions</div>,
}));
vi.mock("../components/AutomationView", () => ({
  default: () => <div data-testid="automation-view">Automation</div>,
}));
vi.mock("../components/JobsView", () => ({
  default: () => <div data-testid="jobs-view">Jobs</div>,
}));
vi.mock("../components/ExportsView", () => ({
  default: () => <div data-testid="exports-view">Exports</div>,
}));
vi.mock("../components/DeadLetterView", () => ({
  default: () => <div data-testid="dead-letter-view">Dead Letter</div>,
}));
vi.mock("../components/DuplicatesView", () => ({
  default: () => <div data-testid="duplicates-view">Duplicates</div>,
}));
vi.mock("../components/BusinessDetailView", () => ({
  default: () => <div data-testid="business-detail-view">Business Detail</div>,
}));

// Mock the API so queries resolve instantly
vi.mock("../api", () => ({
  api: {
    metrics: vi.fn().mockResolvedValue({ businesses: { total: 0 } }),
    automationStatus: vi.fn().mockResolvedValue({ running: false, settings: {}, verification: { running: false } }),
    jobs: vi.fn().mockResolvedValue([]),
    categories: vi.fn().mockResolvedValue([]),
    cities: vi.fn().mockResolvedValue([]),
    exportFiles: vi.fn().mockResolvedValue([]),
    businessLeads: vi.fn().mockResolvedValue([]),
  },
}));

import App from "../App";

let queryClient: QueryClient;

function renderApp() {
  return render(
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  );
}

describe("App", () => {
  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    window.location.hash = "";
  });

  it("renders the sidebar with all navigation items", () => {
    renderApp();
    expect(screen.getByText("Domain Leads")).toBeInTheDocument();
    expect(screen.getByText("Command Center")).toBeInTheDocument();
    // Nav buttons + page heading may duplicate text, so use getAllByText
    expect(screen.getAllByText("Dashboard").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Leads").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Actions").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Automation").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Jobs").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Exports").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Dead Letter").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("Duplicates").length).toBeGreaterThanOrEqual(1);
  });

  it("defaults to dashboard view", () => {
    renderApp();
    expect(screen.getByTestId("dashboard-view")).toBeInTheDocument();
  });

  it("navigates to leads view on click", () => {
    renderApp();
    fireEvent.click(screen.getByText("Leads"));
    expect(screen.getByTestId("leads-view")).toBeInTheDocument();
  });

  it("navigates to jobs view on click", () => {
    renderApp();
    fireEvent.click(screen.getByText("Jobs"));
    expect(screen.getByTestId("jobs-view")).toBeInTheDocument();
  });

  it("navigates to exports view on click", () => {
    renderApp();
    fireEvent.click(screen.getByText("Exports"));
    expect(screen.getByTestId("exports-view")).toBeInTheDocument();
  });

  it("navigates to dead letter view on click", () => {
    renderApp();
    fireEvent.click(screen.getByText("Dead Letter"));
    expect(screen.getByTestId("dead-letter-view")).toBeInTheDocument();
  });

  it("navigates to duplicates view on click", () => {
    renderApp();
    fireEvent.click(screen.getByText("Duplicates"));
    expect(screen.getByTestId("duplicates-view")).toBeInTheDocument();
  });

  it("shows Ready status message", () => {
    renderApp();
    expect(screen.getByText("Ready")).toBeInTheDocument();
  });

  it("shows pipeline status indicators", () => {
    renderApp();
    expect(screen.getByText("Verification OFF")).toBeInTheDocument();
    expect(screen.getByText("Pipeline OFF")).toBeInTheDocument();
  });

  it("reads initial view from URL hash", () => {
    window.location.hash = "jobs";
    renderApp();
    expect(screen.getByTestId("jobs-view")).toBeInTheDocument();
  });

  it("syncs view to URL hash on navigation", () => {
    renderApp();
    fireEvent.click(screen.getByText("Exports"));
    expect(window.location.hash).toBe("#exports");
  });
});
