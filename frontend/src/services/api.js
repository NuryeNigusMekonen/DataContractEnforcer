const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";
const DEFAULT_CACHE_TTL_MS = 12000;
const PAGE_CACHE = new Map();

function withQuery(path, params = {}) {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    searchParams.set(key, String(value));
  });
  const query = searchParams.toString();
  return query ? `${path}?${query}` : path;
}

async function request(path) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed for ${path} with status ${response.status}`);
  }
  return response.json();
}

async function requestJson(path, options) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
    },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${path} with status ${response.status}`);
  }
  return response.json();
}

async function cachedRequest(key, loader, { force = false, ttlMs = DEFAULT_CACHE_TTL_MS } = {}) {
  const now = Date.now();
  const cached = PAGE_CACHE.get(key);
  if (!force && cached?.data !== undefined && now - cached.timestamp < ttlMs) {
    return cached.data;
  }
  if (!force && cached?.promise) {
    return cached.promise;
  }

  const promise = loader()
    .then((data) => {
      PAGE_CACHE.set(key, {
        data,
        timestamp: Date.now(),
      });
      return data;
    })
    .finally(() => {
      const current = PAGE_CACHE.get(key);
      if (current?.promise) {
        PAGE_CACHE.set(key, {
          data: current.data,
          timestamp: current.timestamp || Date.now(),
        });
      }
    });

  PAGE_CACHE.set(key, {
    data: cached?.data,
    timestamp: cached?.timestamp || 0,
    promise,
  });

  return promise;
}

export function invalidateDashboardCache(prefix = "") {
  if (!prefix) {
    PAGE_CACHE.clear();
    return;
  }
  for (const key of PAGE_CACHE.keys()) {
    if (key.startsWith(prefix)) {
      PAGE_CACHE.delete(key);
    }
  }
}

export async function fetchDashboardData({
  violationLimit = 250,
  violationSeverity = "ALL",
} = {}) {
  const [
    summary,
    kpi,
    incident,
    weeks,
    validations,
    violations,
    blame,
    blastRadius,
    schemaEvolution,
    lineageMap,
    timeline,
    whatIf,
    enforcerReport,
    artifacts,
  ] =
    await Promise.all([
      request("/api/summary"),
      request("/api/kpi"),
      request("/api/incidents"),
      request("/api/weeks"),
      request("/api/validations"),
      request(withQuery("/api/violations", { limit: violationLimit, severity: violationSeverity })),
      request("/api/blame"),
      request(withQuery("/api/blast-radius/summary", { limit: 8 })),
      request("/api/schema-evolution"),
      request("/api/lineage-map"),
      request(withQuery("/api/timeline", { limit: 12 })),
      request("/api/what-if/latest"),
      request("/api/enforcer-report"),
      request("/api/artifacts"),
    ]);

  return {
    summary,
    kpi,
    incident,
    weeks,
    validations,
    violations,
    blame,
    blastRadius,
    schemaEvolution,
    lineageMap,
    timeline,
    whatIf,
    enforcerReport,
    artifacts,
  };
}

export function fetchShellData(options = {}) {
  return cachedRequest(
    "page:shell",
    async () => {
      const [kpi, whatIf, artifacts] = await Promise.all([
        request("/api/kpi"),
        request("/api/what-if/latest"),
        request("/api/artifacts"),
      ]);
      return { kpi, whatIf, artifacts };
    },
    options,
  );
}

export function fetchOverviewPageData(options = {}) {
  return cachedRequest(
    "page:overview",
    async () => {
      const [summary, kpi, incident, weeks, violations, timeline, whatIf, enforcerReport] = await Promise.all([
        request("/api/summary"),
        request("/api/kpi"),
        request("/api/incidents"),
        request("/api/weeks"),
        request(withQuery("/api/violations", { limit: 8, severity: "ALL" })),
        request(withQuery("/api/timeline", { limit: 8 })),
        request("/api/what-if/latest"),
        request("/api/enforcer-report"),
      ]);
      return { summary, kpi, incident, weeks, violations, timeline, whatIf, enforcerReport };
    },
    options,
  );
}

export function fetchValidationPageData(options = {}) {
  return cachedRequest(
    "page:validation",
    async () => {
      const [summary, weeks, validations, artifacts] = await Promise.all([
        request("/api/summary"),
        request("/api/weeks"),
        request("/api/validations"),
        request("/api/artifacts"),
      ]);
      return { summary, weeks, validations, artifacts };
    },
    options,
  );
}

export function fetchViolationsPageData(options = {}) {
  return cachedRequest(
    "page:violations",
    async () => {
      const violations = await request(withQuery("/api/violations", { limit: 250, severity: "ALL" }));
      return { violations };
    },
    options,
  );
}

export function fetchAttributionPageData(options = {}) {
  return cachedRequest(
    "page:attribution",
    async () => {
      const [blame, blastRadius, violations, lineageMap] = await Promise.all([
        request("/api/blame"),
        request(withQuery("/api/blast-radius/summary", { limit: 12 })),
        request(withQuery("/api/violations", { limit: 100, severity: "ALL" })),
        request("/api/lineage-map"),
      ]);
      return { blame, blastRadius, violations, lineageMap };
    },
    options,
  );
}

export function fetchSchemaEvolutionPageData(options = {}) {
  return cachedRequest(
    "page:schema",
    async () => {
      const [schemaEvolution, timeline] = await Promise.all([
        request("/api/schema-evolution"),
        request(withQuery("/api/timeline", { limit: 12 })),
      ]);
      return { schemaEvolution, timeline };
    },
    options,
  );
}

export function fetchAiRiskPageData(options = {}) {
  return cachedRequest(
    "page:ai-risk",
    async () => {
      const [enforcerReport, summary] = await Promise.all([
        request("/api/enforcer-report"),
        request("/api/summary"),
      ]);
      return { enforcerReport, summary };
    },
    options,
  );
}

export function fetchLineagePageData(options = {}) {
  return cachedRequest(
    "page:lineage",
    async () => {
      const [lineageMap, violations] = await Promise.all([
        request("/api/lineage-map"),
        request(withQuery("/api/violations", { limit: 100, severity: "ALL" })),
      ]);
      return { lineageMap, violations };
    },
    options,
  );
}

export function fetchArtifactsPageData(options = {}) {
  return cachedRequest(
    "page:artifacts",
    async () => {
      const artifacts = await request("/api/artifacts");
      return { artifacts };
    },
    options,
  );
}

export function fetchLineageMap() {
  return request("/api/lineage-map");
}

export function runWhatIf(changeSpecPath) {
  return requestJson("/api/what-if/run", {
    method: "POST",
    body: JSON.stringify({ change_spec_path: changeSpecPath, async: true }),
  });
}

export function regenerateScenario(scenario) {
  return requestJson("/api/regenerate", {
    method: "POST",
    body: JSON.stringify({ scenario, clear_existing: true, async: true }),
  });
}

export function publishMode(mode) {
  return requestJson("/api/publish", {
    method: "POST",
    body: JSON.stringify({ mode, async: true }),
  });
}

export function injectViolations() {
  return requestJson("/api/inject-violations", {
    method: "POST",
    body: JSON.stringify({ async: true }),
  });
}

export function fetchJob(jobId) {
  return request(`/api/jobs/${jobId}`);
}
