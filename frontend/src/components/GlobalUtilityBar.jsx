import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function shellState(activeJob, shellLoading) {
  if (activeJob) {
    return {
      label: activeJob.status || "running",
      tone: activeJob.status === "failed" ? "FAIL" : activeJob.status === "completed" ? "PASS" : "WARN",
      description: activeJob.description || "Background job in progress.",
    };
  }
  if (shellLoading) {
    return {
      label: "loading",
      tone: "WARN",
      description: "Refreshing global dashboard state.",
    };
  }
  return {
    label: "ready",
    tone: "PASS",
    description: "Global controls are ready.",
  };
}

function UtilityItem({ label, value, meta, tone }) {
  return (
    <article className="shell-utility-item">
      <span className="micro-label">{label}</span>
      <div className="shell-utility-item__value-row">
        <div className="shell-utility-item__value">
          {tone ? <span className={`status-dot status-dot--${getStatusTone(tone)}`} aria-hidden="true" /> : null}
          <strong>{value}</strong>
        </div>
        {tone ? <span className={`badge badge--${getStatusTone(tone)}`}>{tone}</span> : null}
      </div>
      {meta ? <p>{meta}</p> : null}
    </article>
  );
}

function GlobalUtilityBar({ shellData, shellLoading, activeJob, onOpenCommandCenter }) {
  const watcherStatus = shellData?.kpi?.watcher?.status || "UNKNOWN";
  const healthScore = shellData?.kpi?.health_score;
  const lastValidationTime = shellData?.kpi?.last_validation_time;
  const publishedMode = shellData?.artifacts?.mode || "real";
  const scenarioCount = shellData?.kpi?.available_scenarios?.length || 0;
  const specCount = shellData?.whatIf?.available_specs?.length || 0;
  const readiness = shellState(activeJob, shellLoading);

  return (
    <section className="shell-utility-bar" aria-label="Global dashboard status">
      <UtilityItem
        label="Published mode"
        value={publishedMode}
        meta={lastValidationTime ? `Validated ${formatTimestamp(lastValidationTime)}` : "No validation timestamp yet"}
        tone={publishedMode === "violated" ? "WARN" : "PASS"}
      />
      <UtilityItem
        label="Watcher"
        value={watcherStatus}
        meta="Live file monitoring and publish status"
        tone={watcherStatus}
      />
      <UtilityItem
        label="Contract health"
        value={healthScore != null ? `${healthScore}/100` : "--"}
        meta={shellData?.kpi?.health_narrative || "Health score unavailable."}
      />
      <UtilityItem
        label="Scenario catalog"
        value={scenarioCount}
        meta={`${specCount} what-if specifications available`}
      />
      <UtilityItem
        label="Command state"
        value={readiness.label}
        meta={readiness.description}
        tone={readiness.tone}
      />

      <div className="shell-utility-bar__actions">
        <button className="primary-button primary-button--compact" type="button" onClick={onOpenCommandCenter}>
          <span className="button-icon button-icon--panel" aria-hidden="true" />
          Open command center
        </button>
      </div>
    </section>
  );
}

export default GlobalUtilityBar;