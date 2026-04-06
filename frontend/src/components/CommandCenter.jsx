import { useEffect } from "react";

import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { PLATFORM_FULL_NAME, replaceSystemNames } from "../utils/systemNames";

const DEFAULT_FAILURE_SCENARIO_LABEL = "Document Intelligence Confidence Scale Break";
const DEFAULT_CHANGE_SPEC_LABEL = "Document Intelligence Confidence Scale Change";

function StatusCard({ label, value, meta, tone }) {
  return (
    <article className="command-center__status-card">
      <span className="micro-label">{label}</span>
      <div className="command-center__status-row">
        <strong>{value}</strong>
        {tone ? <span className={`badge badge--${getStatusTone(tone)}`}>{tone}</span> : null}
      </div>
      {meta ? <p>{meta}</p> : null}
    </article>
  );
}

function ActionCard({ eyebrow, title, description, children, accent = "neutral" }) {
  return (
    <article className={`command-center__action-card command-center__action-card--${accent}`}>
      <div className="command-center__action-copy">
        <span className="micro-label">{eyebrow}</span>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
      <div className="command-center__action-controls">{children}</div>
    </article>
  );
}

function CommandCenter({
  open,
  onClose,
  shellData,
  shellLoading,
  activeJob,
  failureScenarios,
  selectedScenario,
  onScenarioChange,
  onRestoreHealthy,
  onRunFailureDemo,
  onInjectViolations,
  selectedSpec,
  onSpecChange,
  onRunWhatIf,
}) {
  useEffect(() => {
    if (!open) {
      return undefined;
    }
    function handleKeyDown(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  const watcherStatus = shellData?.kpi?.watcher?.status || "UNKNOWN";
  const publishedMode = shellData?.artifacts?.mode || "real";
  const activeState = activeJob ? activeJob.status : shellLoading ? "loading" : "ready";
  const activeMeta = activeJob?.description || (shellLoading ? "Refreshing shell data" : "No command is running.");
  const scenarioOptions = failureScenarios || [];
  const whatIfSpecs = shellData?.whatIf?.available_specs || [];
  const selectedScenarioOption = scenarioOptions.find((scenario) => scenario.path === selectedScenario);
  const selectedSpecOption = whatIfSpecs.find((spec) => (spec.path || spec.id) === selectedSpec);
  const publishedTone = publishedMode === "violated" ? "WARN" : "PASS";
  const publishedSummary =
    publishedMode === "violated"
      ? "The dashboard is currently showing the published failure state for live review."
      : "The dashboard is aligned to the real publish flow and ready for client walkthroughs.";
  const watcherSummary =
    watcherStatus.toLowerCase() === "watching"
      ? "Backend file watchers are active and shell refreshes are in sync."
      : "Watcher activity should be verified before presenting live dashboard updates.";

  return (
    <>
      <button
        type="button"
        className={`command-center-backdrop ${open ? "command-center-backdrop--open" : ""}`}
        aria-label="Close command center"
        onClick={onClose}
      />

      <aside
        className={`command-center ${open ? "command-center--open" : ""}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby="command-center-title"
      >
        <div className="command-center__header">
          <div className="command-center__header-copy">
            <p className="section-kicker">Global Command Center</p>
            <h2 id="command-center-title">Simulation and publish controls</h2>
            <p className="muted-copy">Four control paths: publish real mode, publish violated mode, failure scenario demo, and what-if simulation.</p>
          </div>
          <button className="toggle-pill" type="button" onClick={onClose}>
            Close
          </button>
        </div>

        <section className="command-center__hero">
          <div className="command-center__hero-copy">
            <span className="micro-label">Client-ready control surface</span>
            <h3>Live state at a glance</h3>
            <p>{publishedSummary}</p>
          </div>
          <div className="command-center__hero-meta">
            <span className={`badge badge--${getStatusTone(publishedTone)}`}>{publishedMode}</span>
            <span className={`badge badge--${getStatusTone(watcherStatus)}`}>{watcherStatus}</span>
            <span className={`badge badge--${getStatusTone(activeState === "ready" ? "PASS" : activeState === "failed" ? "FAIL" : "WARN")}`}>{activeState}</span>
          </div>
        </section>

        <section className="command-center__section command-center__section--surface">
          <div className="command-center__section-head">
            <strong>Operational status</strong>
            <p>Use this panel to confirm what the client is seeing before running any command.</p>
          </div>
          <div className="command-center__status-grid">
            <StatusCard
              label="Published mode"
              value={publishedMode.toUpperCase()}
              meta={shellData?.kpi?.last_validation_time ? `Validated ${formatTimestamp(shellData.kpi.last_validation_time)}` : "Awaiting validation output."}
              tone={publishedTone}
            />
            <StatusCard
              label="Watcher"
              value={watcherStatus}
              meta={watcherSummary}
              tone={watcherStatus}
            />
            <StatusCard
              label="Command state"
              value={activeState}
              meta={activeMeta}
              tone={activeState === "ready" ? "PASS" : activeState === "failed" ? "FAIL" : "WARN"}
            />
          </div>
        </section>

        <section className="command-center__section command-center__section--surface">
          <div className="command-center__section-head">
            <strong>Publish controls</strong>
            <p>These actions mirror the canonical publish flow and are safe entry points for client-facing dashboard updates.</p>
          </div>
          <div className="command-center__action-grid">
            <ActionCard
              eyebrow="Production-aligned"
              title="Publish real mode"
              description={`Run the same ${PLATFORM_FULL_NAME} publish path used by the CLI and refresh the dashboard with the real artifact set.`}
              accent="pass"
            >
              <button className="primary-button" type="button" onClick={onRestoreHealthy}>
                Publish real mode
              </button>
            </ActionCard>
            <ActionCard
              eyebrow="Controlled failure state"
              title="Publish violated baseline"
              description="Switch the dashboard to the committed violated artifact baseline. This is the stable incident state for walkthroughs and enforcement demos." 
              accent="warn"
            >
              <button className="primary-button primary-button--ghost" type="button" onClick={onInjectViolations}>
                Publish violated baseline
              </button>
            </ActionCard>
          </div>
        </section>

        <section className="command-center__section command-center__section--surface">
          <div className="command-center__section-head">
            <strong>Failure scenario demo</strong>
            <p>Scenario regeneration rebuilds demo outputs from the selected scenario. It is variable by scenario and is separate from the fixed violated baseline publish.</p>
          </div>
          <ActionCard
            eyebrow="Scenario-driven regeneration"
            title={selectedScenarioOption ? replaceSystemNames(selectedScenarioOption.label, { short: true }) : DEFAULT_FAILURE_SCENARIO_LABEL}
            description="Use this when you need a custom failure story. The scenario controls which checks and fields fail, so results can differ each run." 
            accent="danger"
          >
            <label className="field command-center__field">
              <span>Scenario</span>
              <select value={selectedScenario} onChange={(event) => onScenarioChange(event.target.value)}>
                {scenarioOptions.map((scenario) => (
                  <option key={scenario.id} value={scenario.path}>
                    {replaceSystemNames(scenario.label, { short: true })}
                  </option>
                ))}
              </select>
            </label>
            <button className="primary-button primary-button--danger" type="button" onClick={onRunFailureDemo} disabled={!selectedScenario}>
              Regenerate failure demo
            </button>
          </ActionCard>
        </section>

        <section className="command-center__section command-center__section--surface">
          <div className="command-center__section-head">
            <strong>What-if simulation</strong>
            <p>Use change specs to preview contract impact before a producer-side change reaches the published dashboard.</p>
          </div>
          <ActionCard
            eyebrow="Pre-deployment analysis"
            title={selectedSpecOption ? replaceSystemNames(selectedSpecOption.label, { short: true }) : DEFAULT_CHANGE_SPEC_LABEL}
            description="Run an impact preview to see whether the proposed change is compatible, adaptable, or breaking before rollout." 
            accent="neutral"
          >
            <label className="field command-center__field">
              <span>Change specification</span>
              <select value={selectedSpec} onChange={(event) => onSpecChange(event.target.value)}>
                {whatIfSpecs.map((spec) => (
                  <option key={spec.id} value={spec.path || spec.id}>
                    {replaceSystemNames(spec.label, { short: true })}
                  </option>
                ))}
              </select>
            </label>
            <button className="primary-button primary-button--ghost" type="button" onClick={onRunWhatIf} disabled={!selectedSpec}>
              Run what-if simulation
            </button>
          </ActionCard>
        </section>
      </aside>
    </>
  );
}

export default CommandCenter;