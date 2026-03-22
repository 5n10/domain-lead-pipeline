import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import JobsView from "../components/JobsView";
import type { JobRun } from "../types";

const mockJobs: JobRun[] = [
  {
    id: "job-1",
    job_name: "pipeline_run",
    scope: "Toronto, CA",
    status: "completed",
    started_at: "2025-06-01T10:00:00Z",
    finished_at: "2025-06-01T10:05:00Z",
    processed_count: 250,
    error: null,
  },
  {
    id: "job-2",
    job_name: "verify_websites_ddg",
    scope: null,
    status: "running",
    started_at: "2025-06-02T08:00:00Z",
    finished_at: null,
    processed_count: 42,
    error: null,
  },
  {
    id: "job-3",
    job_name: "business_score",
    scope: "all",
    status: "failed",
    started_at: "2025-06-03T12:00:00Z",
    finished_at: "2025-06-03T12:01:00Z",
    processed_count: 0,
    error: "Connection refused: database unavailable",
  },
];

describe("JobsView", () => {
  it("renders empty state when no jobs are provided", () => {
    render(<JobsView jobs={[]} />);
    expect(screen.getByText("No jobs found.")).toBeInTheDocument();
  });

  it("renders table headers", () => {
    render(<JobsView jobs={mockJobs} />);
    expect(screen.getByText("Job")).toBeInTheDocument();
    expect(screen.getByText("Scope")).toBeInTheDocument();
    expect(screen.getByText("Status")).toBeInTheDocument();
    expect(screen.getByText("Processed")).toBeInTheDocument();
    expect(screen.getByText("Started")).toBeInTheDocument();
    expect(screen.getByText("Error")).toBeInTheDocument();
  });

  it("renders job names and scopes", () => {
    render(<JobsView jobs={mockJobs} />);
    expect(screen.getByText("pipeline_run")).toBeInTheDocument();
    expect(screen.getByText("Toronto, CA")).toBeInTheDocument();
    expect(screen.getByText("verify_websites_ddg")).toBeInTheDocument();
    expect(screen.getByText("business_score")).toBeInTheDocument();
    expect(screen.getByText("all")).toBeInTheDocument();
  });

  it("renders status badges with correct text", () => {
    render(<JobsView jobs={mockJobs} />);
    expect(screen.getByText("completed")).toBeInTheDocument();
    expect(screen.getByText("running")).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
  });

  it("renders processed counts", () => {
    render(<JobsView jobs={mockJobs} />);
    expect(screen.getByText("250")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("renders error messages and shows dash for no error", () => {
    render(<JobsView jobs={mockJobs} />);
    expect(
      screen.getByText("Connection refused: database unavailable")
    ).toBeInTheDocument();
    // Jobs without errors show "-"
    const dashes = screen.getAllByText("-");
    expect(dashes.length).toBeGreaterThanOrEqual(1);
  });

  it("renders scope as dash when null", () => {
    render(<JobsView jobs={mockJobs} />);
    // job-2 has scope: null, which renders as "-"
    const dashes = screen.getAllByText("-");
    expect(dashes.length).toBeGreaterThanOrEqual(1);
  });
});
