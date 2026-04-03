import { useEffect, useRef, useState } from "react";

import BlastRadiusPanel from "../components/BlastRadiusPanel";
import BlamePanel from "../components/BlamePanel";
import IncidentCard from "../components/IncidentCard";
import KPICards from "../components/KPICards";
import SchemaEvolutionCard from "../components/SchemaEvolutionCard";
import TimelinePanel from "../components/TimelinePanel";
import ViolationsTable from "../components/ViolationsTable";
import WeekStatusGrid from "../components/WeekStatusGrid";
import WhatIfPanel from "../components/WhatIfPanel";
import { fetchDashboardData, fetchJob, regenerateScenario, runWhatIf } from "../services/api";

const REFRESH_INTERVAL_MS = 5000;
const EMPTY_STATE = {
  kpi: {},
  incident: {},
  weeks: [],
  violations: [],
  blastRadius: { top_fields: [], all_fields: [] },
  blame: { items: [] },
  schemaEvolution: { items: [] },
  whatIf: {},
  timeline: { items: [] },
};
const SECTION_KEYS = ["kpi", "incident", "violations", "blastRadius", "blame", "schemaEvolution", "whatIf", "timeline"];

function signatureFor(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch (error) {
    return String(value);
  }
}

function Dashboard() {
  const [data, setData] = useState(EMPTY_STATE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastRefresh, setLastRefresh] = useState("");
  const [updatedSections, setUpdatedSections] = useState(
    SECTION_KEYS.reduce((accumulator, key) => ({ ...accumulator, [key]: false }), {}),
  );
  const [updatedWeekKeys, setUpdatedWeekKeys] = useState([]);
  const [runningWhatIf, setRunningWhatIf] = useState(false);
  const [runningRegenerate, setRunningRegenerate] = useState(false);
  const [selectedScenario, setSelectedScenario] = useState("");
  const [severityFilter, setSeverityFilter] = useState("ALL");
  const [search, setSearch] = useState("");
  const [showAllViolations, setShowAllViolations] = useState(false);
  const [showAllBlame, setShowAllBlame] = useState(false);
  const [showFullTimeline, setShowFullTimeline] = useState(false);
  const [jobs, setJobs] = useState({});
  const dataRef = useRef(EMPTY_STATE);
  const flashTimeouts = useRef({});
  const initialLoadComplete = useRef(false);
  const previousWeeksRef = useRef([]);

  function flashSection(sectionKey) {
    window.clearTimeout(flashTimeouts.current[sectionKey]);
    setUpdatedSections((current) => ({ ...current, [sectionKey]: true }));
    flashTimeouts.current[sectionKey] = window.setTimeout(() => {
      setUpdatedSections((current) => ({ ...current, [sectionKey]: false }));
    }, 1800);
  }

  function applyDashboardData(nextData) {
    const previousWeeks = previousWeeksRef.current;
    const changedWeeks = nextData.weeks
      .filter((week) => {
        const previousWeek = previousWeeks.find((candidate) => candidate.key === week.key);
        return previousWeek && previousWeek.last_updated !== week.last_updated;
      })
      .map((week) => week.key);
    setUpdatedWeekKeys(changedWeeks);
    previousWeeksRef.current = nextData.weeks;

    if (initialLoadComplete.current) {
      SECTION_KEYS.forEach((sectionKey) => {
        if (signatureFor(dataRef.current[sectionKey]) !== signatureFor(nextData[sectionKey])) {
          flashSection(sectionKey);
        }
      });
    } else {
      initialLoadComplete.current = true;
    }

    dataRef.current = nextData;
    setData(nextData);
    setLastRefresh(new Date().toLocaleTimeString());
    setError("");
  }

  async function loadDashboard() {
    const nextData = await fetchDashboardData({
      violationLimit: showAllViolations ? 200 : 10,
      violationSeverity: severityFilter,
      blameLimit: showAllBlame ? 50 : 3,
      timelineLimit: showFullTimeline ? 20 : 8,
    });
    applyDashboardData({ ...EMPTY_STATE, ...nextData });
  }

  useEffect(() => {
    let active = true;

    async function load() {
      try {
        if (!initialLoadComplete.current) {
          setLoading(true);
        }
        const nextData = await fetchDashboardData({
          violationLimit: showAllViolations ? 200 : 10,
          violationSeverity: severityFilter,
          blameLimit: showAllBlame ? 50 : 3,
          timelineLimit: showFullTimeline ? 20 : 8,
        });
        if (!active) {
          return;
        }
        applyDashboardData({ ...EMPTY_STATE, ...nextData });
      } catch (requestError) {
        if (active) {
          setError(requestError.message || "Failed to refresh dashboard data.");
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
  }, [severityFilter, showAllViolations, showAllBlame, showFullTimeline]);

  useEffect(() => {
    if (!selectedScenario && data.kpi.available_scenarios?.length) {
      setSelectedScenario(data.kpi.available_scenarios[0].path);
    }
  }, [data.kpi.available_scenarios, selectedScenario]);

  useEffect(() => {
    const activeEntries = Object.entries(jobs).filter(([, job]) =>
      job && ["queued", "running"].includes(job.status),
    );
    if (!activeEntries.length) {
      return undefined;
    }

    const intervalId = window.setInterval(async () => {
      const updates = await Promise.all(
        activeEntries.map(async ([key, job]) => [key, await fetchJob(job.job_id)]),
      );

      for (const [key, job] of updates) {
        setJobs((current) => ({ ...current, [key]: job }));
        if (job.status === "completed") {
          await loadDashboard();
        }
        if (job.status === "failed") {
          setError(job.error || `${key} job failed.`);
        }
      }
    }, 1500);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [jobs]);

  async function handleRunWhatIf(changeSpecPath) {
    try {
      setRunningWhatIf(true);
      const job = await runWhatIf(changeSpecPath);
      setJobs((current) => ({ ...current, whatIf: job }));
    } catch (requestError) {
      setError(requestError.message || "What-if simulation failed.");
    } finally {
      setRunningWhatIf(false);
    }
  }

  async function handleRegenerate() {
    if (!selectedScenario) {
      return;
    }
    try {
      setRunningRegenerate(true);
      const job = await regenerateScenario(selectedScenario);
      setJobs((current) => ({ ...current, regenerate: job }));
    } catch (requestError) {
      setError(requestError.message || "Output regeneration failed.");
    } finally {
      setRunningRegenerate(false);
    }
  }

  const jobMessages = Object.values(jobs)
    .filter((job) => job && ["queued", "running"].includes(job.status))
    .map((job) => job.description);
  const isWhatIfBusy = runningWhatIf || ["queued", "running"].includes(jobs.whatIf?.status);
  const isRegenerateBusy = runningRegenerate || ["queued", "running"].includes(jobs.regenerate?.status);

  return (
    <main className="dashboard-shell">
      <header className="dashboard-header">
        <div>
          <p className="section-kicker">Week 7 Dashboard</p>
          <h1>Enterprise contract control panel</h1>
          <p className="header-copy">
            Decisions first, root cause second. This view surfaces the highest-risk contract issues,
            downstream exposure, and the next action without flooding the screen with raw validation noise.
          </p>
        </div>
        <div className="header-status">
          <span className="live-indicator">Auto-refresh every 5 seconds</span>
          <strong>{loading ? "Refreshing..." : `Last refresh ${lastRefresh || "Unavailable"}`}</strong>
          <p>{data.kpi.watcher?.status || "Watcher starting"}</p>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}
      {jobMessages.length ? <div className="job-banner">{jobMessages.join(" • ")}</div> : null}

      <section className="operations-bar">
        <div>
          <p className="section-kicker">Operations</p>
          <h2>Scenario regeneration</h2>
        </div>
        <div className="toolbar-row toolbar-row--compact">
          <label className="field field--grow">
            <span>Scenario</span>
            <select value={selectedScenario} onChange={(event) => setSelectedScenario(event.target.value)}>
              {(data.kpi.available_scenarios || []).map((scenario) => (
                <option key={scenario.id} value={scenario.path}>
                  {scenario.label}
                </option>
              ))}
            </select>
          </label>
          <button className="primary-button" type="button" onClick={handleRegenerate} disabled={isRegenerateBusy || !selectedScenario}>
            {isRegenerateBusy ? "Regenerating..." : "Regenerate outputs"}
          </button>
        </div>
      </section>

      <KPICards kpi={data.kpi} isUpdated={updatedSections.kpi} />

      <section className="split-grid">
        <IncidentCard incident={data.incident} isUpdated={updatedSections.incident} />
        <WhatIfPanel whatIf={data.whatIf} onRun={handleRunWhatIf} running={isWhatIfBusy} isUpdated={updatedSections.whatIf} />
      </section>

      <section className="split-grid split-grid--overview">
        <WeekStatusGrid weeks={data.weeks} updatedWeekKeys={updatedWeekKeys} />
        <SchemaEvolutionCard schemaEvolution={data.schemaEvolution} isUpdated={updatedSections.schemaEvolution} />
      </section>

      <ViolationsTable
        violations={data.violations}
        severityFilter={severityFilter}
        search={search}
        onSeverityChange={setSeverityFilter}
        onSearchChange={setSearch}
        onToggleExpanded={() => setShowAllViolations((current) => !current)}
        expanded={showAllViolations}
        isUpdated={updatedSections.violations}
      />

      <section className="split-grid">
        <BlastRadiusPanel blastRadius={data.blastRadius} isUpdated={updatedSections.blastRadius} />
        <BlamePanel
          blame={data.blame}
          expanded={showAllBlame}
          onToggleExpanded={() => setShowAllBlame((current) => !current)}
          isUpdated={updatedSections.blame}
        />
      </section>

      <TimelinePanel
        timeline={data.timeline}
        expanded={showFullTimeline}
        onToggleExpanded={() => setShowFullTimeline((current) => !current)}
        isUpdated={updatedSections.timeline}
      />
    </main>
  );
}

export default Dashboard;
