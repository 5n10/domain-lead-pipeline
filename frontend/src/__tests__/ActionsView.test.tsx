import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import ActionsView from "../components/ActionsView";
import { ExportProvider } from "../contexts/ExportContext";

const noop = () => {};
const noopAsync = async () => {};

function renderActionsView(overrides: Partial<Parameters<typeof ActionsView>[0]> = {}) {
  const defaultProps = {
    actionLoading: false,
    setActionLoading: noop as (v: boolean) => void,
    setStatusMessage: noop as (s: string) => void,
    refresh: noopAsync,
  };
  return render(
    <ExportProvider>
      <ActionsView {...defaultProps} {...overrides} />
    </ExportProvider>
  );
}

describe("ActionsView", () => {
  it("renders verification tool cards", () => {
    renderActionsView();
    expect(screen.getByText("Verification Tools")).toBeInTheDocument();
    expect(screen.getByText("Domain Guess")).toBeInTheDocument();
    expect(screen.getByText("SearXNG Verify")).toBeInTheDocument();
    expect(screen.getByText("DDG Search Verify")).toBeInTheDocument();
    expect(screen.getByText("Google Search Verify")).toBeInTheDocument();
    expect(screen.getByText("LLM Verify")).toBeInTheDocument();
    expect(screen.getByText("Google Places Verify")).toBeInTheDocument();
    expect(screen.getByText("Foursquare Verify")).toBeInTheDocument();
    expect(screen.getByText("Domain Validation (RDAP)")).toBeInTheDocument();
  });

  it("renders enrichment tool cards", () => {
    renderActionsView();
    expect(screen.getByText("Enrichment Tools")).toBeInTheDocument();
    expect(screen.getByText("Google Places Enrich")).toBeInTheDocument();
    expect(screen.getByText("Foursquare Enrich")).toBeInTheDocument();
    expect(screen.getByText("Hunter.io Emails")).toBeInTheDocument();
  });

  it("renders pipeline form with area input", () => {
    renderActionsView();
    expect(screen.getByText("Run Full Pipeline")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("e.g. Toronto, Canada")).toBeInTheDocument();
    expect(screen.getByText("Run Pipeline")).toBeInTheDocument();
  });

  it("renders export form with platform input", () => {
    renderActionsView();
    expect(screen.getByText("Export Businesses")).toBeInTheDocument();
    expect(screen.getByText("Platform")).toBeInTheDocument();
    expect(screen.getByText("Export Now")).toBeInTheDocument();
  });

  it("renders scoring form", () => {
    renderActionsView();
    expect(screen.getByText("Score Businesses")).toBeInTheDocument();
    expect(screen.getByText("Score Now")).toBeInTheDocument();
    expect(screen.getByText("Force rescore")).toBeInTheDocument();
  });

  it("shows 'Running...' on action card buttons when actionLoading is true", () => {
    renderActionsView({ actionLoading: true });
    // ActionCard buttons display "Running..." when loading
    const runningButtons = screen.getAllByText("Running...");
    expect(runningButtons.length).toBeGreaterThan(0);
  });

  it("renders bulk actions section", () => {
    renderActionsView();
    expect(screen.getByText("Bulk Actions")).toBeInTheDocument();
    expect(screen.getByText("Run Deduplication")).toBeInTheDocument();
    expect(screen.getByText("Bulk Score All")).toBeInTheDocument();
    expect(screen.getByText("Bulk Rescore All")).toBeInTheDocument();
  });

  it("renders utilities section", () => {
    renderActionsView();
    expect(screen.getByText("Utilities")).toBeInTheDocument();
    expect(screen.getByText("Export to Sheets")).toBeInTheDocument();
    expect(screen.getByText("Test Notification")).toBeInTheDocument();
  });
});
