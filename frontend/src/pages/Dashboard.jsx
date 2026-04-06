import { useEffect, useMemo, useRef, useState } from "react";

import AIRiskPanel from "../components/AIRiskPanel";
import ArtifactsPanel from "../components/ArtifactsPanel";
import BlameChainPanel from "../components/BlameChainPanel";
import CoverageMatrix from "../components/CoverageMatrix";
import DemoModeBar from "../components/DemoModeBar";
import IncidentPanel from "../components/IncidentPanel";
import LineageGraphPanel from "../components/LineageGraphPanel";
import MetricOverview, { buildOverviewItems } from "../components/MetricOverview";
import SchemaEvolutionPanel from "../components/SchemaEvolutionPanel";
import TrendPanel from "../components/TrendPanel";
import ViolationWorkbench from "../components/ViolationWorkbench";
import {
  fetchDashboardData,
  fetchJob,
  regenerateScenario,
  runWhatIf,
} from "../services/api";

const REFRESH_INTERVAL_MS = 7000;
const EMPTY_LINEAGE = {
  status: "missing",
  captured_at: null,
  last_updated: null,
  full: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
  cross_week: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
};
const EMPTY_STATE = {
  summary: {},
  kpi: {},
  incident: {},
  weeks: [],
  validations: [],
  violations: [],
  blame: [],
  blastRadius: {},
  schemaEvolution: {},
  lineageMap: EMPTY_LINEAGE,
  timeline: { items: [] },
  whatIf: { available_specs: [] },
  enforcerReport: {},
  artifacts: {
    contracts: [],
    validation_reports: [],
    schema_snapshots: [],
    violation_logs: [],
    report_files: [],
    sample_contract_clauses: [],
  },
};

function signatureFor(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch (error) {
    return String(value);
  }
}

function aiRiskStatus(aiRisk) {
  const structuredOutput = aiRisk?.structured_llm_output_enforcement || aiRisk?.llm_output_schema_rate;
  const statuses = [
    aiRisk?.embedding_drift?.status,
    aiRisk?.prompt_input_validation?.status,
    structuredOutput?.status,
    aiRisk?.langsmith_trace_schema_contracts?.status,
  ].map((value) => String(value || "UNKNOWN").toUpperCase());
  if (statuses.some((value) => ["FAIL", "ERROR", "CRITICAL", "HIGH"].includes(value))) {
    return "FAIL";
  }
  if (statuses.some((value) => value === "WARN")) {
    return "WARN";
  }
  if (statuses.some((value) => value === "PASS")) {
    return "PASS";
  }
  return "UNKNOWN";
}

function healthScore(passed, warned, total) {
  if (!total) {
    return 100;
  }
  return Math.max(0, Math.min(100, Math.round(((passed + warned * 0.5) / total) * 100)));
}

function inferDriftScore(item) {
  const base = (item.change_count || 0) * 15;
  const warnScore = Number(item.warned || item.checks_warned || 0) * 2;
  const failScore = Number(item.failed || item.checks_failed || 0) * 4;
  return Math.min(100, base + warnScore + failScore);
}

function buildTrendItems(artifacts, weeks) {
  const reportMap = new Map((artifacts.validation_reports || []).map((item) => [item.contract_id, item]));
  return weeks.map((week) => {
    const report = reportMap.get(week.contract_id);
    const passed = Number(report?.passed ?? week.checks_passed ?? 0);
    const warned = Number(report?.warned ?? week.checks_warned ?? 0);
    const failed = Number(report?.failed ?? week.checks_failed ?? 0);
    const total = Number(report?.total_checks ?? week.total_checks ?? 0);
    return {
      label: week.week_name,
      status: week.status,
      passed,
      warned,
      failed,
      healthScore: report?.health_score ?? healthScore(passed, warned, total),
      driftScore: report?.drift_score ?? inferDriftScore(week),
    };
  });
}

function inferUpstreamSystem(path, contractId) {
  const candidate = String(path || contractId || "").toLowerCase();
  if (candidate.includes("week1")) {
    return "week1-intent-tracker";
  }
  if (candidate.includes("week2")) {
    return "week2-digital-courtroom";
  }
  if (candidate.includes("week3")) {
    return "week3-document-refinery";
  }
  if (candidate.includes("week4")) {
    return "week4-brownfield-cartographer";
  }
  if (candidate.includes("week5")) {
    return "week5-ledger";
  }
  if (candidate.includes("trace")) {
    return "langsmith-trace-pipeline";
  }
  return "unknown-producer";
}

function buildCoverageRows(contracts, weeks, schemaEvolution) {
  const weekMap = new Map((weeks || []).map((item) => [item.contract_id, item]));
  return (contracts || []).map((contract) => {
    const week = weekMap.get(contract.contract_id) || {};
    return {
      contractId: contract.contract_id,
      contractName: contract.title || contract.contract_id,
      dataset: contract.dataset,
      upstreamSystem: inferUpstreamSystem(contract.source_path, contract.contract_id),
      downstreamSystem: contract.downstream_labels?.length
        ? contract.downstream_labels.join(", ")
        : "No downstream consumers",
      validationActive: Boolean(week.total_checks),
      attributionActive: contract.downstream_count > 0,
      schemaEvolutionTracking: Boolean(schemaEvolution.contract_id) || Boolean(week.contract_id),
      aiExtensionsApplied: Boolean(contract.ai_extensions_applied),
      status: week.status || "UNKNOWN",
    };
  });
}

function fallbackIncidentFromTimeline(timeline) {
  const lastViolation = (timeline.items || []).find((item) => item.category === "violation");
  if (!lastViolation) {
    return {
      severity: "PASS",
      title: "No recent resolved incident",
      message: "Recent runs are healthy and no historical violation event is available for fallback display.",
      detected_at: null,
      field: null,
      affected_systems: [],
    };
  }
  return {
    severity: lastViolation.status,
    title: lastViolation.title,
    message: lastViolation.details,
    detected_at: lastViolation.timestamp || lastViolation.time,
    field: lastViolation.source,
    week: lastViolation.title,
    affected_systems: [],
  };
}

function computeFocusNodeIds(selectedViolation, lineageMap) {
  if (!selectedViolation) {
    return [];
  }
  const weekToken = String(selectedViolation.week || "")
    .toLowerCase()
    .replace(/\s+/g, "");
  const affectedSystems = (selectedViolation.affected_systems || []).map((item) => String(item).toLowerCase());

  return (lineageMap.cross_week?.nodes || [])
    .filter((node) => {
      const haystack = `${node.id} ${node.label} ${node.path}`.toLowerCase();
      if (weekToken && haystack.includes(weekToken)) {
        return true;
      }
      if (affectedSystems.some((system) => haystack.includes(system))) {
        return true;
      }
      return haystack.includes("week7-validation-runner") || haystack.includes("week7-violation-attributor");
    })
    .map((node) => node.id);
}

function preferredScenarioPath(scenarios, fragment) {
  return scenarios.find((scenario) => scenario.path.includes(fragment))?.path || scenarios[0]?.path || "";
}

function Dashboard() {
  const [data, setData] = useState(EMPTY_STATE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastRefresh, setLastRefresh] = useState("");
  const [selectedViolation, setSelectedViolation] = useState(null);
  const [selectedScenario, setSelectedScenario] = useState("");
  const [selectedSpec, setSelectedSpec] = useState("");
  const [jobs, setJobs] = useState({});
  const dataRef = useRef(EMPTY_STATE);

  async function loadDashboard() {
    const payload = await fetchDashboardData();
    dataRef.current = { ...EMPTY_STATE, ...payload };
    setData(dataRef.current);
    setLastRefresh(new Date().toLocaleTimeString());
    setError("");
  }

  useEffect(() => {
    let active = true;

    async function load() {
      try {
        if (!dataRef.current.kpi?.health_score) {
          setLoading(true);
        }
        const payload = await fetchDashboardData();
        if (!active) {
          return;
        }
        dataRef.current = { ...EMPTY_STATE, ...payload };
        setData(dataRef.current);
        setLastRefresh(new Date().toLocaleTimeString());
        setError("");
      } catch (requestError) {
        if (active) {
          setError(requestError.message || "Failed to load dashboard data.");
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    }

    load();
    const intervalId = window.setInterval(load, REFRESH_INTERVAL_MS);
    return () => {
      active = false;
      window.clearInterval(intervalId);
    };
  }, []);

  useEffect(() => {
    if (!selectedViolation && data.violations.length) {
      setSelectedViolation(data.violations[0]);
      return;
    }
    if (selectedViolation && !data.violations.some((item) => item.violation_id === selectedViolation.violation_id)) {
      setSelectedViolation(data.violations[0] || null);
    }
  }, [data.violations, selectedViolation]);

  useEffect(() => {
    if (!selectedScenario && data.kpi.available_scenarios?.length) {
      setSelectedScenario(preferredScenarioPath(data.kpi.available_scenarios, "week3_confidence_scale_break"));
    }
  }, [data.kpi.available_scenarios, selectedScenario]);

  useEffect(() => {
    if (!selectedSpec && data.whatIf.available_specs?.length) {
      setSelectedSpec(
        data.whatIf.available_specs.find((spec) => String(spec.id || spec.path).includes("confidence_scale_change"))?.path
        || data.whatIf.available_specs[0].path
        || data.whatIf.available_specs[0].id
        || "",
      );
    }
  }, [data.whatIf.available_specs, selectedSpec]);

  useEffect(() => {
    const activeJobs = Object.values(jobs).filter((job) => job && ["queued", "running"].includes(job.status));
    if (!activeJobs.length) {
      return undefined;
    }
    const intervalId = window.setInterval(async () => {
      const refreshed = await Promise.all(activeJobs.map((job) => fetchJob(job.job_id)));
      for (const job of refreshed) {
        setJobs((current) => ({ ...current, [job.kind]: job }));
        if (job.status === "completed") {
          await loadDashboard();
        }
        if (job.status === "failed") {
          setError(job.error || `${job.kind} failed.`);
        }
      }
    }, 1500);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [jobs]);

  async function queueJob(kind, task) {
    try {
      const job = await task();
      setJobs((current) => ({ ...current, [kind]: job }));
    } catch (requestError) {
      setError(requestError.message || `${kind} request failed.`);
    }
  }

  const aiRisk = data.enforcerReport.ai_system_risk_assessment || {};
  const overviewItems = buildOverviewItems({
    kpi: data.kpi,
    summary: data.summary,
    violations: data.violations,
    aiRiskStatus: aiRiskStatus(aiRisk),
  });
  const trendItems = buildTrendItems(data.artifacts, data.weeks);
  const coverageRows = buildCoverageRows(data.artifacts.contracts, data.weeks, data.schemaEvolution);
  const fallbackIncident = fallbackIncidentFromTimeline(data.timeline);
  const focusNodeIds = useMemo(
    () => computeFocusNodeIds(selectedViolation, data.lineageMap),
    [data.lineageMap, selectedViolation],
  );
  const activeJob = Object.values(jobs).find((job) => job && ["queued", "running"].includes(job.status));
  const updatedSinceLastLoad = signatureFor(dataRef.current) !== signatureFor(data);

  return (
    <main className="dashboard-shell dashboard-shell--production">
      <header className="dashboard-header dashboard-header--hero">
        <div>
          <p className="section-kicker">Week 7 Data Contract Enforcer</p>
          <h1>Production dashboard for validation, lineage, schema change, and AI risk evidence</h1>
          <p className="header-copy">
            Monitor end-to-end data health, inspect breaking changes, trace downstream impact,
            and demonstrate failure scenarios without changing the current backend contract flow.
          </p>
        </div>
        <div className="header-status header-status--stacked">
          <span className="live-indicator">Auto-refresh every 7 seconds</span>
          <strong>{loading ? "Refreshing…" : `Last refresh ${lastRefresh || "Unavailable"}`}</strong>
          <p>Published mode {data.artifacts.mode || "real"} • watcher {data.kpi.watcher?.status || "starting"}</p>
          <p>{updatedSinceLastLoad ? "Fresh data loaded into the dashboard." : "Live monitoring active."}</p>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <DemoModeBar
        scenarios={data.kpi.available_scenarios || []}
        specs={data.whatIf.available_specs || []}
        selectedScenario={selectedScenario}
        onScenarioChange={setSelectedScenario}
        onRestoreHealthy={() => queueJob("regenerate", () => regenerateScenario(preferredScenarioPath(data.kpi.available_scenarios || [], "healthy.yaml")))}
        onRunFailureDemo={() => queueJob("regenerate", () => regenerateScenario(selectedScenario))}
        selectedSpec={selectedSpec}
        onSpecChange={setSelectedSpec}
        onRunWhatIf={() => queueJob("what_if", () => runWhatIf(selectedSpec))}
        busyLabel={activeJob?.description || ""}
      />

      <MetricOverview items={overviewItems} />

      <section className="dashboard-two-column">
        <IncidentPanel incident={data.incident} fallbackIncident={fallbackIncident} />
        <TrendPanel items={trendItems} />
      </section>

      <CoverageMatrix rows={coverageRows} />

      <section className="dashboard-two-column dashboard-two-column--dense">
        <ViolationWorkbench
          violations={data.violations}
          selectedViolation={selectedViolation}
          onSelect={setSelectedViolation}
        />
        <BlameChainPanel
          blame={data.blame}
          selectedViolation={selectedViolation}
          blastRadius={data.blastRadius}
        />
      </section>

      <section className="dashboard-two-column">
        <SchemaEvolutionPanel schemaEvolution={data.schemaEvolution} />
        <AIRiskPanel aiRisk={aiRisk} />
      </section>

      <LineageGraphPanel
        lineageMap={data.lineageMap}
        viewportHeight={620}
        focusNodeIds={focusNodeIds}
        focusLabel={
          selectedViolation
            ? `Highlighted path for ${selectedViolation.field || "selected violation"} across the current lineage graph.`
            : "Select a violation to highlight its likely affected path."
        }
      />

      <ArtifactsPanel artifacts={data.artifacts} />
    </main>
  );
}

export default Dashboard;
