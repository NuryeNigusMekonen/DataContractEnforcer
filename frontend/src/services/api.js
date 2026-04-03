const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";

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

export async function fetchDashboardData({
  violationLimit = 10,
  violationSeverity = "ALL",
  blameLimit = 3,
  timelineLimit = 8,
} = {}) {
  const [kpi, incident, weeks, violations, blastRadius, blame, schemaEvolution, whatIf, timeline] =
    await Promise.all([
      request("/api/kpi"),
      request("/api/incidents"),
      request("/api/weeks"),
      request(withQuery("/api/violations", { limit: violationLimit, severity: violationSeverity })),
      request(withQuery("/api/blast-radius/summary", { limit: 5 })),
      request(withQuery("/api/blame/top", { limit: blameLimit })),
      request("/api/schema-evolution"),
      request("/api/what-if/latest"),
      request(withQuery("/api/timeline", { limit: timelineLimit })),
    ]);

  return {
    kpi,
    incident,
    weeks,
    violations,
    blastRadius,
    blame,
    schemaEvolution,
    whatIf,
    timeline,
  };
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

export function fetchJob(jobId) {
  return request(`/api/jobs/${jobId}`);
}
