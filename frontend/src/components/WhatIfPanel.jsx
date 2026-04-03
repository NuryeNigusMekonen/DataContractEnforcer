import { useEffect, useState } from "react";

import { formatChangeSummary, formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function ComparisonBlock({ label, status, context }) {
  return (
    <article className="comparison-block">
      <span>{label}</span>
      <strong>{status || "UNKNOWN"}</strong>
      <p>{context}</p>
    </article>
  );
}

function WhatIfPanel({ whatIf, onRun, running, isUpdated = false }) {
  const specs = whatIf.available_specs || [];
  const [selectedSpec, setSelectedSpec] = useState(specs[0]?.path || specs[0]?.id || "");

  useEffect(() => {
    if (!selectedSpec && specs.length > 0) {
      setSelectedSpec(specs[0].path || specs[0].id || "");
    }
  }, [selectedSpec, specs]);

  async function handleSubmit() {
    if (!selectedSpec || running) {
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
          {whatIf.final_verdict || "UNKNOWN"}
        </span>
      </div>

      <div className="toolbar-row">
        <label className="field field--grow">
          <span>Proposed change</span>
          <select value={selectedSpec} onChange={(event) => setSelectedSpec(event.target.value)}>
            {specs.map((spec) => (
              <option key={spec.id} value={spec.path || spec.id}>
                {spec.label}
              </option>
            ))}
          </select>
        </label>
        <button className="primary-button" type="button" onClick={handleSubmit} disabled={running || !selectedSpec}>
          {running ? "Running..." : "Run simulation"}
        </button>
      </div>

      <article className="emphasis-card">
        <span>Proposed change</span>
        <strong>{formatChangeSummary(whatIf.proposed_change)}</strong>
      </article>

      <div className="comparison-grid">
        <ComparisonBlock
          label="Baseline status"
          status={whatIf.baseline_status}
          context={`${formatCompactNumber(whatIf.baseline_summary?.failed_checks ?? 0)} failed checks`}
        />
        <ComparisonBlock
          label="Raw changed status"
          status={whatIf.raw_status}
          context={`${formatCompactNumber(whatIf.raw_summary?.failed_checks ?? 0)} failed checks`}
        />
        <ComparisonBlock
          label="Adapter recovery status"
          status={whatIf.adapter_status}
          context={`${whatIf.adapter_summary?.rules_applied ?? 0} rules applied`}
        />
        <ComparisonBlock
          label="Final verdict"
          status={whatIf.final_verdict}
          context={whatIf.compatibility_verdict || "Compatibility pending"}
        />
      </div>

      <div className="detail-split">
        <article className="list-card">
          <strong>Affected systems</strong>
          <p>{whatIf.affected_systems?.join(", ") || "No downstream systems listed"}</p>
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
