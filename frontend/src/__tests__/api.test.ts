import { describe, it, expect, vi, beforeEach } from "vitest";

// Set up fetch mock before importing the api module so that
// import.meta.env values are undefined and API_BASE defaults to
// "http://127.0.0.1:8000".
const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

import { api } from "../api";

const BASE = "http://127.0.0.1:8000";

function mockOkResponse(data: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    json: () => Promise.resolve(data),
    text: () => Promise.resolve(JSON.stringify(data)),
  };
}

function mockErrorResponse(status: number, statusText: string, body: string) {
  return {
    ok: false,
    status,
    statusText,
    json: () => Promise.reject(new Error("not json")),
    text: () => Promise.resolve(body),
  };
}

describe("api", () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  // ---------- 1. api.metrics() ----------
  it("metrics() calls /api/metrics and returns parsed JSON", async () => {
    const data = { businesses: { total: 42 } };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.metrics();

    expect(mockFetch).toHaveBeenCalledOnce();
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/metrics`);
    expect(opts.headers).toEqual(
      expect.objectContaining({ "Content-Type": "application/json" }),
    );
    expect(result).toEqual(data);
  });

  // ---------- 2. api.jobs(10) ----------
  it("jobs(10) passes limit=10 as query param", async () => {
    const data = [{ id: "j1", job_name: "scrape" }];
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.jobs(10);

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/jobs?limit=10`);
    expect(result).toEqual(data);
  });

  // ---------- 3. api.categories() ----------
  it("categories() calls /api/leads/business/categories", async () => {
    const data = ["Plumber", "Electrician"];
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.categories();

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/leads/business/categories`);
    expect(result).toEqual(data);
  });

  // ---------- 4. api.cities() ----------
  it("cities() includes limit=500 in the URL", async () => {
    const data = ["Amsterdam", "Rotterdam"];
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.cities();

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/leads/business/cities?limit=500`);
    expect(result).toEqual(data);
  });

  // ---------- 5. api.businessLeads(params) ----------
  it("businessLeads(params) passes URLSearchParams into the URL", async () => {
    const data = { total_candidates: 5, returned: 2, items: [] };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const params = new URLSearchParams({ category: "Plumber", limit: "20" });
    const result = await api.businessLeads(params);

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(
      `${BASE}/api/leads/business?category=Plumber&limit=20`,
    );
    expect(result).toEqual(data);
  });

  // ---------- 6. api.exportFiles() ----------
  it("exportFiles() calls /api/exports/files", async () => {
    const data = [{ name: "export.csv", size: 1024, modified_at: 1 }];
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.exportFiles();

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/exports/files`);
    expect(result).toEqual(data);
  });

  // ---------- 7. api.runPipeline(payload) ----------
  it("runPipeline(payload) uses POST, sends JSON body, includes Content-Type header", async () => {
    const payload = { area: "Amsterdam", categories: "Plumber" };
    const responseData = { status: "started" };
    mockFetch.mockResolvedValueOnce(mockOkResponse(responseData));

    const result = await api.runPipeline(payload);

    expect(mockFetch).toHaveBeenCalledOnce();
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/actions/pipeline-run`);
    expect(opts.method).toBe("POST");
    expect(opts.body).toBe(JSON.stringify(payload));
    expect(opts.headers).toEqual(
      expect.objectContaining({ "Content-Type": "application/json" }),
    );
    expect(result).toEqual(responseData);
  });

  // ---------- 8. api.scoreBusinesses(payload) ----------
  it("scoreBusinesses(payload) sends POST with JSON body", async () => {
    const payload = { min_score: 50 };
    const responseData = { processed: 10 };
    mockFetch.mockResolvedValueOnce(mockOkResponse(responseData));

    const result = await api.scoreBusinesses(payload);

    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/actions/business-score`);
    expect(opts.method).toBe("POST");
    expect(opts.body).toBe(JSON.stringify(payload));
    expect(result).toEqual(responseData);
  });

  // ---------- 9. api.automationStatus() ----------
  it("automationStatus() is a GET request with no auth header", async () => {
    const data = { running: false, busy: false };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.automationStatus();

    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/automation/status`);
    // GET request — no method set explicitly, so method should be undefined
    expect(opts.method).toBeUndefined();
    // No X-API-Key header for GET requests (MUTATION_API_KEY is empty string
    // since env var is not set)
    expect(opts.headers).not.toHaveProperty("X-API-Key");
    expect(result).toEqual(data);
  });

  // ---------- 10. api.automationStart(payload) ----------
  it("automationStart(payload) sends POST with JSON body", async () => {
    const payload = { interval_seconds: 300 };
    const responseData = { running: true, busy: false };
    mockFetch.mockResolvedValueOnce(mockOkResponse(responseData));

    const result = await api.automationStart(payload);

    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/automation/start`);
    expect(opts.method).toBe("POST");
    expect(opts.body).toBe(JSON.stringify(payload));
    expect(result).toEqual(responseData);
  });

  // ---------- 11. api.automationStop() ----------
  it("automationStop() sends POST with no body", async () => {
    const responseData = { running: false, busy: false };
    mockFetch.mockResolvedValueOnce(mockOkResponse(responseData));

    const result = await api.automationStop();

    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/automation/stop`);
    expect(opts.method).toBe("POST");
    expect(result).toEqual(responseData);
  });

  // ---------- 12. api.deadLetter(100, 50) ----------
  it("deadLetter(100, 50) passes limit and offset as query params", async () => {
    const data = { total: 3, returned: 3, items: [] };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.deadLetter(100, 50);

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/dead-letter?limit=100&offset=50`);
    expect(result).toEqual(data);
  });

  // ---------- 13. api.businessDetail("some-id") ----------
  it('businessDetail("some-id") includes the ID in the URL', async () => {
    const data = { id: "some-id", name: "Acme Corp" };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    const result = await api.businessDetail("some-id");

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/leads/business/some-id`);
    expect(result).toEqual(data);
  });

  // ---------- 14. Non-ok response throws Error ----------
  it("throws Error with status, statusText and body on non-ok response", async () => {
    mockFetch.mockResolvedValueOnce(
      mockErrorResponse(400, "Bad Request", "invalid payload"),
    );

    await expect(api.metrics()).rejects.toThrow(
      "400 Bad Request: invalid payload",
    );
  });

  // ---------- 15. api.validateDomains() ----------
  it("validateDomains({sync_limit: 100}) builds query string instead of JSON body", async () => {
    const responseData = { queued: 100 };
    mockFetch.mockResolvedValueOnce(mockOkResponse(responseData));

    const result = await api.validateDomains({ sync_limit: 100 });

    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe(
      `${BASE}/api/actions/validate-domains?sync_limit=100`,
    );
    expect(opts.method).toBe("POST");
    // validateDomains should NOT send a JSON body — params go in the URL
    expect(opts.body).toBeUndefined();
    expect(result).toEqual(responseData);
  });

  // ---------- 16. jobs() default limit ----------
  it("jobs() defaults to limit=50 when no argument provided", async () => {
    const data: unknown[] = [];
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    await api.jobs();

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/jobs?limit=50`);
  });

  // ---------- 17. deadLetter() default params ----------
  it("deadLetter() defaults to limit=200 and offset=0", async () => {
    const data = { total: 0, returned: 0, items: [] };
    mockFetch.mockResolvedValueOnce(mockOkResponse(data));

    await api.deadLetter();

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/dead-letter?limit=200&offset=0`);
  });

  // ---------- 18. validateDomains with multiple params ----------
  it("validateDomains with multiple params builds correct query string", async () => {
    mockFetch.mockResolvedValueOnce(mockOkResponse({}));

    await api.validateDomains({
      sync_limit: 50,
      rdap_limit: 25,
      rescore: true,
    });

    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("sync_limit=50");
    expect(url).toContain("rdap_limit=25");
    expect(url).toContain("rescore=true");
  });

  // ---------- 19. Non-ok response for POST throws Error ----------
  it("throws Error on non-ok response for POST requests", async () => {
    mockFetch.mockResolvedValueOnce(
      mockErrorResponse(500, "Internal Server Error", "something broke"),
    );

    await expect(api.runPipeline({ area: "test" })).rejects.toThrow(
      "500 Internal Server Error: something broke",
    );
  });

  // ---------- 20. GET requests do not include X-API-Key ----------
  it("GET requests never include X-API-Key header", async () => {
    mockFetch.mockResolvedValueOnce(mockOkResponse([]));

    await api.categories();

    const [, opts] = mockFetch.mock.calls[0];
    expect(opts.headers).not.toHaveProperty("X-API-Key");
  });

  // ---------- 21. validateDomains with empty payload ----------
  it("validateDomains({}) produces URL with no query string", async () => {
    mockFetch.mockResolvedValueOnce(mockOkResponse({}));

    await api.validateDomains({});

    const [url] = mockFetch.mock.calls[0];
    expect(url).toBe(`${BASE}/api/actions/validate-domains`);
  });

  // ---------- 22. baseUrl exposes the API base ----------
  it("baseUrl property equals the default API base", () => {
    expect(api.baseUrl).toBe(BASE);
  });
});
