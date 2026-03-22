import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import DashboardView from "../components/DashboardView";
import type { Metrics } from "../types";

const mockMetrics: Metrics = {
  businesses: {
    total: 1000,
    no_website: 500,
    scored: 300,
    no_website_scored: 200,
    no_website_unscored: 100,
  },
  domains: { hosted: 50, unhosted: 30 },
  contacts: { total: 400, scored: 200, unscored: 200 },
  exports: { total: 10, queued: 5 },
  business_exports: { total: 15, queued: 3 },
  verification: { any_source: 100, domain_guess: 80, searxng: 60 },
  verification_details: { ddg_conclusive: 50, ddg_no_results: 10 },
  confidence_distribution: { high: 50, medium: 30, low: 10, unverified: 10 },
  recent_jobs: [],
};

describe("DashboardView", () => {
  it("renders loading state when metrics is null", () => {
    render(<DashboardView metrics={null} />);
    expect(screen.getByText("Loading metrics...")).toBeInTheDocument();
  });

  it("renders metric cards with correct values", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(screen.getByText("Total Businesses")).toBeInTheDocument();
    // "No Website" appears in both the metric card and the funnel
    expect(screen.getAllByText("No Website").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("Scored (No Website)")).toBeInTheDocument();
    expect(screen.getByText("Business Exports")).toBeInTheDocument();

    // Check the numeric values rendered by MetricCard (toLocaleString)
    // "1,000" appears in both the metric card and the funnel bar
    expect(screen.getAllByText("1,000").length).toBeGreaterThanOrEqual(1);
    // "500" appears in the metric card value and inside the funnel bar
    expect(screen.getAllByText("500").length).toBeGreaterThanOrEqual(1);
    // "200" appears in Scored (No Website) card and funnel
    expect(screen.getAllByText("200").length).toBeGreaterThanOrEqual(1);
    // "15" appears in both the Business Exports metric card and the funnel "Exported" bar
    expect(screen.getAllByText("15").length).toBeGreaterThanOrEqual(1);
  });

  it("renders verification coverage badges", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(screen.getByText("Verification Coverage")).toBeInTheDocument();

    // Badges render as "Label: count"
    expect(screen.getByText("Any Verified: 100")).toBeInTheDocument();
    expect(screen.getByText("Domain Guess: 80")).toBeInTheDocument();
    expect(screen.getByText("SearXNG: 60")).toBeInTheDocument();
  });

  it("renders confidence distribution bars", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(screen.getByText("Confidence Distribution")).toBeInTheDocument();

    // Labels for each confidence level
    expect(screen.getByText("High")).toBeInTheDocument();
    expect(screen.getByText("Medium")).toBeInTheDocument();
    expect(screen.getByText("Low")).toBeInTheDocument();
    // "Unverified" appears in both confidence distribution and funnel description, so use getAll
    expect(screen.getAllByText("Unverified").length).toBeGreaterThanOrEqual(1);

    // Counts next to bars (rendered as toLocaleString)
    // "50" may appear in multiple places (confidence bar + DDG conclusive text)
    expect(screen.getAllByText("50").length).toBeGreaterThanOrEqual(1);
    // "30" may appear in funnel conversion percentages too
    expect(screen.getAllByText("30").length).toBeGreaterThanOrEqual(1);
    // Both "low" (10) and "unverified" (10) have count 10, so there are multiple
    expect(screen.getAllByText("10").length).toBeGreaterThanOrEqual(2);
  });

  it("renders domain status entries", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(screen.getByText("Domain Status")).toBeInTheDocument();

    // Domain entries render as "status: count"
    expect(screen.getByText("hosted: 50")).toBeInTheDocument();
    expect(screen.getByText("unhosted: 30")).toBeInTheDocument();
  });

  it("renders the verification funnel with expected stages", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(screen.getByText("Verification Pipeline Funnel")).toBeInTheDocument();

    // Funnel stage labels
    expect(screen.getByText("Total")).toBeInTheDocument();
    // "No Website" already tested above but also appears in funnel
    expect(screen.getByText("Verified")).toBeInTheDocument();
    expect(screen.getByText("Confident")).toBeInTheDocument();
    expect(screen.getByText("Exported")).toBeInTheDocument();
  });

  it("renders DDG verification details", () => {
    render(<DashboardView metrics={mockMetrics} />);
    expect(
      screen.getByText(/DDG breakdown: 50 conclusive, 10 inconclusive/)
    ).toBeInTheDocument();
  });
});
