import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import LeadsView, { defaultFilters } from "../components/LeadsView";
import type { BusinessLead, BusinessLeadResponse, Metrics } from "../types";

const mockLead: BusinessLead = {
  id: "test-1",
  name: "Test Biz",
  category: "trades",
  address: "123 Main St",
  city: "Toronto",
  country: "CA",
  lead_score: 75,
  scored_at: "2025-01-01",
  source: "osm",
  source_id: "node/123",
  emails: ["a@b.com"],
  business_emails: ["a@b.com"],
  free_emails: [],
  phones: ["+1234"],
  domains: ["test.com"],
  verified_unhosted_domains: [],
  unregistered_domains: [],
  unknown_domains: [],
  hosted_domains: [],
  parked_domains: [],
  domain_status_counts: {},
  exported: false,
  verification_count: 2,
  verification_sources: ["domain_guess", "searxng"],
  verification_confidence: "high",
};

const mockLeadsResponse: BusinessLeadResponse = {
  total_candidates: 150,
  returned: 1,
  items: [mockLead],
};

const mockMetrics: Metrics = {
  businesses: { total: 1000, no_website: 500, scored: 300, no_website_scored: 200, no_website_unscored: 100 },
  domains: { hosted: 50, unhosted: 30 },
  contacts: { total: 400, scored: 200, unscored: 200 },
  exports: { total: 10, queued: 5 },
  business_exports: { total: 15, queued: 3 },
  verification: { any_source: 100 },
  verification_details: { ddg_conclusive: 50, ddg_no_results: 10 },
  confidence_distribution: { high: 50, medium: 30, low: 10, unverified: 10 },
  recent_jobs: [],
};

const noop = () => {};

function renderLeadsView(overrides: Partial<Parameters<typeof LeadsView>[0]> = {}) {
  const defaultProps = {
    leads: mockLeadsResponse,
    metrics: mockMetrics,
    filters: defaultFilters,
    categories: ["trades", "food"],
    cities: ["Toronto", "Vancouver"],
    loading: false,
    onFiltersChange: noop as (f: typeof defaultFilters) => void,
    onApply: noop,
    onSelectBusiness: noop,
  };
  return render(<LeadsView {...defaultProps} {...overrides} />);
}

describe("LeadsView", () => {
  it("renders 'No leads found' when leads is null", () => {
    renderLeadsView({ leads: null });
    expect(screen.getByText(/No leads found/)).toBeInTheDocument();
  });

  it("renders 'No leads found' when items is empty", () => {
    renderLeadsView({ leads: { total_candidates: 0, returned: 0, items: [] } });
    expect(screen.getByText(/No leads found/)).toBeInTheDocument();
  });

  it("renders table headers", () => {
    renderLeadsView();
    expect(screen.getByText("Name")).toBeInTheDocument();
    // "Category" appears in both the table header and the filter label
    expect(screen.getAllByText("Category").length).toBeGreaterThanOrEqual(1);
    // "City" appears in both the table header and a filter label
    expect(screen.getAllByText("City").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("Score")).toBeInTheDocument();
    expect(screen.getByText("Confidence")).toBeInTheDocument();
    expect(screen.getByText("Verified")).toBeInTheDocument();
    expect(screen.getByText("Email")).toBeInTheDocument();
    expect(screen.getByText("Phone")).toBeInTheDocument();
    expect(screen.getByText("Domain")).toBeInTheDocument();
    expect(screen.getByText("Exported")).toBeInTheDocument();

    // Verify the headers exist inside a thead via columnheader role
    const columnHeaders = screen.getAllByRole("columnheader");
    const headerTexts = columnHeaders.map((h) => h.textContent);
    expect(headerTexts).toContain("Name");
    expect(headerTexts).toContain("Category");
    expect(headerTexts).toContain("City");
    expect(headerTexts).toContain("Score");
  });

  it("renders lead rows with correct data", () => {
    renderLeadsView();
    expect(screen.getByText("Test Biz")).toBeInTheDocument();
    // "trades" appears in both the category <option> and the <td>
    expect(screen.getAllByText("trades").length).toBeGreaterThanOrEqual(1);
    // "Toronto" also appears in the cities datalist
    expect(screen.getAllByText("Toronto").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("75")).toBeInTheDocument();
    expect(screen.getByText("a@b.com")).toBeInTheDocument();
    expect(screen.getByText("+1234")).toBeInTheDocument();
    expect(screen.getByText("test.com")).toBeInTheDocument();
    expect(screen.getByText("No")).toBeInTheDocument();

    // Verify the data row cell contents via table rows
    const rows = screen.getAllByRole("row");
    // Row 0 = header, Row 1 = data row
    expect(rows.length).toBeGreaterThanOrEqual(2);
    const dataRow = rows[1];
    expect(dataRow.textContent).toContain("Test Biz");
    expect(dataRow.textContent).toContain("trades");
    expect(dataRow.textContent).toContain("Toronto");
  });

  it("shows candidate count summary", () => {
    renderLeadsView();
    expect(screen.getByText("1")).toBeInTheDocument(); // returned count
    expect(screen.getByText("150")).toBeInTheDocument(); // total_candidates
  });

  it("Apply button exists and can be clicked", () => {
    const onApply = vi.fn();
    renderLeadsView({ onApply });
    const button = screen.getByText("Apply");
    expect(button).toBeInTheDocument();
    fireEvent.click(button);
    expect(onApply).toHaveBeenCalledTimes(1);
  });

  it("renders filter checkboxes", () => {
    renderLeadsView();
    expect(screen.getByText("Require contact")).toBeInTheDocument();
    expect(screen.getByText("No website only")).toBeInTheDocument();
    expect(screen.getByText("Exclude hosted email domains")).toBeInTheDocument();
    expect(screen.getByText("Require unhosted domain")).toBeInTheDocument();
    expect(screen.getByText("Require domain qualification")).toBeInTheDocument();
    expect(screen.getByText("Only unexported")).toBeInTheDocument();
    expect(screen.getByText("Only verified")).toBeInTheDocument();
  });
});
