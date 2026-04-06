import { useEffect, useState } from "react";

import { formatChangeSummary, formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { formatSystemList, replaceSystemNames } from "../utils/systemNames";

function formatStatusLabel(value) {
  return String(value || "UNKNOWN").replace(/_/g, " ");
}

function ComparisonBlock({ label, status, context }) {
  return (
    <article className="comparison-block">
      <span>{label}</span>
      <strong>{formatStatusLabel(status)}</strong>
      <p>{context}</p>
    </article>
  );
}

function WhatIfPanel({ whatIf, onRun, running, isUpdated = false }) {
  const specs = whatIf.available_specs || [];
  const canRun = typeof onRun === "function";
  const isMigrationCase = whatIf.raw_status === "PASS" && whatIf.final_verdict === "BREAKING_REQUIRES_MIGRATION";
  const resolutionLabel = isMigrationCase ? "Migration path" : "Adapter recovery";
  const resolutionStatus = isMigrationCase
    ? "REQUIRED"
    : whatIf.adapter_attempted
      ? (whatIf.adapter_status || "UNKNOWN")
      : "NO_ADAPTER_NEEDED";
  const resolutionContext = isMigrationCase
    ? "Current payload still passes under today's consumer contract; rollout still needs an approved subscriber migration plan."
    : whatIf.adapter_attempted
      ? `${whatIf.adapter_summary?.rules_applied ?? 0} rules applied`
      : "No adapter is needed for the current payload shape.";
  const rawLabel = isMigrationCase ? "Current consumer replay" : "Raw changed status";
  const rawContext = isMigrationCase
    ? "Changed payload still passes against the currently deployed consumer contract."
    : `${formatCompactNumber(whatIf.raw_summary?.failed_checks ?? 0)} failed checks`;
  const finalContext = isMigrationCase
    ? "Schema change requires subscriber migration before rollout."
    : formatStatusLabel(whatIf.compatibility_verdict || "Compatibility pending");
  const [selectedSpec, setSelectedSpec] = useState(specs[0]?.path || specs[0]?.id || "");

  useEffect(() => {
    if (!selectedSpec && specs.length > 0) {
      setSelectedSpec(specs[0].path || specs[0].id || "");
    }
  }, [selectedSpec, specs]);

  async function handleSubmit() {
    if (!canRun || !selectedSpec || running) {
      return;
    }
    await onRun(selectedSpec);
  }

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Incident And Simulation</p>
          <h2>What-if simulation</h2>
        </div>
        <span className={`badge badge--${getStatusTone(whatIf.final_verdict)}`}>
          {formatStatusLabel(whatIf.final_verdict)}
        </span>
      </div>

      {canRun ? (
        <div className="toolbar-row">
          <label className="field field--grow">
            <span>Proposed change</span>
            <select value={selectedSpec} onChange={(event) => setSelectedSpec(event.target.value)}>
              {specs.map((spec) => (
                <option key={spec.id} value={spec.path || spec.id}>
                  {replaceSystemNames(spec.label, { short: true })}
                </option>
              ))}
            </select>
          </label>
          <button className="primary-button" type="button" onClick={handleSubmit} disabled={running || !selectedSpec}>
            {running ? "Running..." : "Run simulation"}
          </button>
        </div>
      ) : null}

      <article className="emphasis-card">
        <span>Proposed change</span>
        <strong>{replaceSystemNames(formatChangeSummary(whatIf.proposed_change))}</strong>
      </article>

      <div className="comparison-grid">
        <ComparisonBlock
          label="Baseline status"
          status={whatIf.baseline_status}
          context={`${formatCompactNumber(whatIf.baseline_summary?.failed_checks ?? 0)} failed checks`}
        />
        <ComparisonBlock
          label={rawLabel}
          status={whatIf.raw_status}
          context={rawContext}
        />
        <ComparisonBlock
          label={resolutionLabel}
          status={resolutionStatus}
          context={resolutionContext}
        />
        <ComparisonBlock
          label="Final verdict"
          status={whatIf.final_verdict}
          context={finalContext}
        />
      </div>

      <div className="detail-split">
        <article className="list-card">
          <strong>Downstream systems</strong>
          <p>{formatSystemList(whatIf.affected_systems, { empty: "No downstream systems listed" })}</p>
        </article>
        <article className="list-card">
          <strong>Recommendation</strong>
          <p>{whatIf.recommendation || "No recommendation available."}</p>
        </article>
      </div>

      <p className="muted-copy">Updated {formatTimestamp(whatIf.last_updated)}</p>
    </section>
  );
}

export default WhatIfPanel;
