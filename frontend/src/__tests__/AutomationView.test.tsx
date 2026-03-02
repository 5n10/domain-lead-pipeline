import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import AutomationView from "../components/AutomationView";
import { ExportProvider } from "../contexts/ExportContext";
import { AutomationSettingsProvider } from "../contexts/AutomationContext";
import type { AutomationStatus } from "../types";

const noop = () => {};
const noopAsync = async () => {};

const mockAutomation: AutomationStatus = {
  running: false,
  busy: false,
  settings: {
    interval_seconds: 900,
    area: null,
    categories: "all",
    rdap_statuses: [],
    daily_target_enabled: true,
    daily_target_count: 100,
    daily_target_min_score: 40,
    daily_target_platform_prefix: "daily",
    daily_target_require_contact: true,
    daily_target_require_domain_qualification: true,
    daily_target_require_unhosted_domain: false,
    daily_target_allow_recycle: true,
  },
  last_run_started_at: null,
  last_run_finished_at: null,
  last_error: null,
  last_result: null,
  run_count: 5,
  verification: {
    running: false,
    settings: {
      domain_guess_batch: 500,
      domain_guess_min_score: 0,
      ddg_batch: 30,
      ddg_min_score: 30,
      llm_batch: 30,
      llm_min_score: 30,
      google_search_batch: 20,
      google_search_min_score: 30,
      searxng_batch: 200,
      searxng_min_score: 0,
      rescore_after_batch: true,
      pause_between_batches: 3,
      pause_when_idle: 60,
    },
    last_started_at: null,
    last_finished_at: null,
    last_error: null,
    batch_count: 0,
    totals: {
      domain_guess_processed: 0,
      domain_guess_websites: 0,
      ddg_processed: 0,
      ddg_websites: 0,
      llm_processed: 0,
      llm_websites: 0,
      google_search_processed: 0,
      google_search_websites: 0,
      searxng_processed: 0,
      searxng_websites: 0,
      rescored: 0,
    },
  },
};

function renderAutomationView(overrides: { automation?: AutomationStatus | null } = {}) {
  const automation = overrides.automation !== undefined ? overrides.automation : mockAutomation;
  const defaultProps = {
    automation,
    setAutomation: noop as (s: AutomationStatus) => void,
    actionLoading: false,
    setActionLoading: noop as (v: boolean) => void,
    setStatusMessage: noop as (s: string) => void,
    refresh: noopAsync,
  };
  return render(
    <ExportProvider>
      <AutomationSettingsProvider automation={automation}>
        <AutomationView {...defaultProps} />
      </AutomationSettingsProvider>
    </ExportProvider>
  );
}

describe("AutomationView", () => {
  it("renders 'Continuous Verification' heading", () => {
    renderAutomationView();
    expect(screen.getByText("Continuous Verification")).toBeInTheDocument();
  });

  it("shows Start button when verification is not running", () => {
    renderAutomationView();
    const startButtons = screen.getAllByText("Start");
    expect(startButtons.length).toBeGreaterThanOrEqual(1);
  });

  it("shows Stop button when verification is running", () => {
    const runningAutomation: AutomationStatus = {
      ...mockAutomation,
      verification: {
        ...mockAutomation.verification!,
        running: true,
      },
    };
    renderAutomationView({ automation: runningAutomation });
    const stopButtons = screen.getAllByText("Stop");
    expect(stopButtons.length).toBeGreaterThanOrEqual(1);
  });

  it("renders pipeline automation section", () => {
    renderAutomationView();
    expect(screen.getByText("Pipeline Automation")).toBeInTheDocument();
    expect(screen.getByText("Run Cycle Now")).toBeInTheDocument();
    expect(screen.getByText("Generate Daily Target")).toBeInTheDocument();
  });

  it("shows run count", () => {
    renderAutomationView();
    expect(screen.getByText(/Runs: 5/)).toBeInTheDocument();
  });

  it("renders pipeline settings section", () => {
    renderAutomationView();
    expect(screen.getByText("Pipeline Settings")).toBeInTheDocument();
    expect(screen.getByText("Save Pipeline Settings")).toBeInTheDocument();
    expect(screen.getByText("Daily target enabled")).toBeInTheDocument();
    expect(screen.getByText("Allow lead recycle")).toBeInTheDocument();
  });

  it("renders verification layer rows", () => {
    renderAutomationView();
    expect(screen.getByText("Domain Guess")).toBeInTheDocument();
    expect(screen.getByText("SearXNG")).toBeInTheDocument();
    expect(screen.getByText("DDG Search")).toBeInTheDocument();
    expect(screen.getByText("Google Search")).toBeInTheDocument();
  });
});
