import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import ExportsView from "../components/ExportsView";
import type { ExportFile } from "../types";

vi.mock("../api", () => ({
  api: {
    baseUrl: "http://127.0.0.1:8000",
  },
}));

const mockFiles: ExportFile[] = [
  {
    name: "leads_2025-06-01.csv",
    size: 2048,
    modified_at: 1717200000, // epoch seconds
  },
  {
    name: "leads_2025-06-02.csv",
    size: 1048576, // 1 MB
    modified_at: 1717286400,
  },
  {
    name: "daily_target_2025-06-03.csv",
    size: 512,
    modified_at: 1717372800,
  },
];

describe("ExportsView", () => {
  it("renders empty state when no files are provided", () => {
    render(<ExportsView files={[]} />);
    expect(screen.getByText("No export files found.")).toBeInTheDocument();
  });

  it("renders file cards for each export file", () => {
    render(<ExportsView files={mockFiles} />);
    expect(screen.getByText("leads_2025-06-01.csv")).toBeInTheDocument();
    expect(screen.getByText("leads_2025-06-02.csv")).toBeInTheDocument();
    expect(screen.getByText("daily_target_2025-06-03.csv")).toBeInTheDocument();
  });

  it("renders CSV badge for each file", () => {
    render(<ExportsView files={mockFiles} />);
    const csvBadges = screen.getAllByText("CSV");
    expect(csvBadges).toHaveLength(3);
  });

  it("formats file sizes correctly", () => {
    render(<ExportsView files={mockFiles} />);
    // 2048 bytes = "2.0 KB"
    expect(screen.getByText(/2\.0 KB/)).toBeInTheDocument();
    // 1048576 bytes = "1.0 MB"
    expect(screen.getByText(/1\.0 MB/)).toBeInTheDocument();
    // 512 bytes = "512 B"
    expect(screen.getByText(/512 B/)).toBeInTheDocument();
  });

  it("renders download links pointing to the API", () => {
    render(<ExportsView files={mockFiles} />);
    const links = screen.getAllByRole("link");
    expect(links).toHaveLength(3);

    expect(links[0]).toHaveAttribute(
      "href",
      "http://127.0.0.1:8000/api/exports/files/leads_2025-06-01.csv"
    );
    expect(links[1]).toHaveAttribute(
      "href",
      "http://127.0.0.1:8000/api/exports/files/leads_2025-06-02.csv"
    );
    expect(links[2]).toHaveAttribute(
      "href",
      "http://127.0.0.1:8000/api/exports/files/daily_target_2025-06-03.csv"
    );
  });

  it("opens links in a new tab with noreferrer", () => {
    render(<ExportsView files={mockFiles} />);
    const links = screen.getAllByRole("link");
    for (const link of links) {
      expect(link).toHaveAttribute("target", "_blank");
      expect(link).toHaveAttribute("rel", "noreferrer");
    }
  });
});
