import { useMemo, useState } from "react";

import { formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function severityOrder(value) {
  const normalized = String(value || "").toUpperCase();
  if (normalized === "CRITICAL") {
    return 4;
  }
  if (normalized === "HIGH") {
    return 3;
  }
  if (normalized === "MEDIUM") {
    return 2;
  }
  if (normalized === "LOW") {
    return 1;
  }
  return 0;
}

function DetailBlock({ label, value }) {
  return (
    <article className="detail-block">
      <span className="micro-label">{label}</span>
      <strong>{value || "n/a"}</strong>
    </article>
  );
}

function SampleList({ items = [] }) {
  if (!items.length) {
    return <p className="muted-copy">No failing samples were captured for this violation. Run a failure scenario if you want richer payload evidence.</p>;
  }
  return (
    <div className="sample-stack">
      {items.map((item, index) => (
        <pre className="code-preview" key={`${index}-${JSON.stringify(item)}`}>{JSON.stringify(item, null, 2)}</pre>
      ))}
    </div>
  );
}

function ViolationWorkbench({ violations = [], selectedViolation, onSelect, onNavigate }) {
  const [search, setSearch] = useState("");
  const [severityFilter, setSeverityFilter] = useState("ALL");

  const filteredViolations = useMemo(() => {
    const query = search.trim().toLowerCase();
    return violations.filter((violation) => {
      if (severityFilter !== "ALL" && String(violation.severity || "").toUpperCase() !== severityFilter) {
        return false;
      }
      if (!query) {
        return true;
      }
      return [violation.field, violation.week, replaceSystemNames(violation.week), replaceSystemNames(violation.week, { short: true }), violation.message, violation.check_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query));
    });
  }, [search, severityFilter, violations]);

  const severityCounts = useMemo(() => {
    return violations.reduce((accumulator, violation) => {
      const key = String(violation.severity || "UNKNOWN").toUpperCase();
      accumulator[key] = (accumulator[key] || 0) + 1;
      return accumulator;
    }, {});
  }, [violations]);

  const highestVisibleSeverity = useMemo(() => {
    if (!filteredViolations.length) {
      return "PASS";
    }
    const candidate = [...filteredViolations].sort(
      (left, right) => severityOrder(right.severity) - severityOrder(left.severity),
    )[0];
    return String(candidate?.severity || "WARN").toUpperCase();
  }, [filteredViolations]);

  const operatorAction = useMemo(() => {
    if (highestVisibleSeverity === "CRITICAL") {
      return "Critical incident view: use Attribution to identify ownership and Lineage to confirm downstream impact before any publish action.";
    }
    if (highestVisibleSeverity === "HIGH") {
      return "High-risk incident view: confirm failure evidence, then validate mitigation path in Attribution and What-if before rollout.";
    }
    if (!filteredViolations.length) {
      return "No violations match current filters. Clear filters or run a failure demo to inspect enforcement behavior.";
    }
    return "Operational review mode: inspect evidence, then follow linked investigations for root cause and blast radius validation.";
  }, [filteredViolations.length, highestVisibleSeverity]);

  const affectedSystemsLabel = useMemo(() => {
    const systems = selectedViolation?.affected_systems || [];
    if (!systems.length) {
      return "No downstream systems listed";
    }
    return systems.map((item) => replaceSystemNames(String(item))).join(", ");
  }, [selectedViolation]);

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Violation Deep Dive</p>
          <h2>Violation evidence and failing samples</h2>
          <p className="muted-copy">Filter the incident stream, select a failure, then move directly into attribution or lineage without losing context.</p>
        </div>
      </div>

      <div className="workbench-summary-bar">
        <article className="summary-pill">
          <span className="micro-label">Visible</span>
          <strong>{formatCompactNumber(filteredViolations.length)}</strong>
        </article>
        <article className="summary-pill">
          <span className="micro-label">Critical</span>
          <strong>{formatCompactNumber(severityCounts.CRITICAL || 0)}</strong>
        </article>
        <article className="summary-pill">
          <span className="micro-label">High</span>
          <strong>{formatCompactNumber(severityCounts.HIGH || 0)}</strong>
        </article>
      </div>

      <article className={`incident-hero incident-hero--${getStatusTone(highestVisibleSeverity)}`}>
        <span className="micro-label">Operator action</span>
        <strong>{operatorAction}</strong>
      </article>

      <div className="toolbar-row">
        <label className="field">
          <span>Severity</span>
          <select value={severityFilter} onChange={(event) => setSeverityFilter(event.target.value)}>
            <option value="ALL">All severities</option>
            <option value="CRITICAL">Critical</option>
            <option value="HIGH">High</option>
            <option value="MEDIUM">Medium</option>
            <option value="LOW">Low</option>
          </select>
        </label>
        <label className="field field--grow">
          <span>Search</span>
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Field, week, message, or check id"
          />
        </label>
      </div>

      <div className="workbench-grid">
        <div className="table-shell">
          <table className="data-table data-table--selectable">
            <thead>
              <tr>
                <th>Field</th>
                <th>Severity</th>
                <th>Week</th>
                <th>Check</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {filteredViolations.map((violation) => (
                <tr
                  key={violation.violation_id}
                  className={selectedViolation?.violation_id === violation.violation_id ? "is-selected" : ""}
                  onClick={() => onSelect(violation)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      onSelect(violation);
                    }
                  }}
                  tabIndex={0}
                >
                  <td>{violation.field || "Unknown field"}</td>
                  <td><span className={`badge badge--${getStatusTone(violation.severity)}`}>{violation.severity || "UNKNOWN"}</span></td>
                  <td>{getSystemDisplayName(violation.week, { short: true, fallback: replaceSystemNames(violation.week, { short: true }) || "Unknown" })}</td>
                  <td>{violation.check_id || "n/a"}</td>
                  <td>{replaceSystemNames(violation.short_message || violation.message)}</td>
                </tr>
              ))}
              {!filteredViolations.length ? (
                <tr>
                  <td colSpan="5" className="table-empty">No violations match the current filters. Clear the search or run a failure scenario to generate test incidents.</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>

        <aside className="detail-panel">
          {selectedViolation ? (
            <>
              <div className="detail-panel-head">
                <div>
                  <p className="section-kicker">Selected Violation</p>
                  <h3>{selectedViolation.field || "Unknown field"}</h3>
                  <p className="muted-copy">{selectedViolation.violation_id}</p>
                </div>
                <span className={`badge badge--${getStatusTone(selectedViolation.severity)}`}>
                  {selectedViolation.severity || "UNKNOWN"}
                </span>
              </div>

              <section className="detail-section">
                <div className="section-rule-heading">
                  <span className="micro-label">Summary</span>
                </div>
                <p className="detail-summary">{selectedViolation.message}</p>
              </section>

              <section className="detail-section">
                <div className="section-rule-heading">
                  <span className="micro-label">Metadata</span>
                </div>
                <div className="detail-inline-grid">
                  <article className="inline-fact">
                    <span className="micro-label">Check ID</span>
                    <strong>{selectedViolation.check_id || "n/a"}</strong>
                  </article>
                  <article className="inline-fact">
                    <span className="micro-label">Detected</span>
                    <strong>{formatTimestamp(selectedViolation.detected_at)}</strong>
                  </article>
                  <article className="inline-fact">
                    <span className="micro-label">Failing records</span>
                    <strong>{formatCompactNumber(selectedViolation.records_failing)}</strong>
                  </article>
                  <article className="inline-fact">
                    <span className="micro-label">Affected systems</span>
                    <strong>{formatCompactNumber(selectedViolation.affected_systems_count)}</strong>
                  </article>
                </div>
                <article className="list-card list-card--soft">
                  <strong>Affected system names</strong>
                  <p>{affectedSystemsLabel}</p>
                </article>
              </section>

              <section className="detail-section">
                <div className="section-rule-heading">
                  <span className="micro-label">Expectation delta</span>
                </div>
                <div className="detail-split">
                  <article className="list-card list-card--soft">
                    <strong>Expected</strong>
                    <p>{String(selectedViolation.expected || "n/a")}</p>
                  </article>
                  <article className="list-card list-card--soft">
                    <strong>Actual</strong>
                    <p>{typeof selectedViolation.actual === "string" ? selectedViolation.actual : JSON.stringify(selectedViolation.actual)}</p>
                  </article>
                </div>
              </section>

              <section className="detail-section">
                <div className="section-rule-heading">
                  <span className="micro-label">Failing samples</span>
                </div>
                <article className="list-card list-card--soft">
                  <SampleList items={selectedViolation.sample_records || []} />
                </article>
              </section>

              <section className="detail-section">
                <div className="section-rule-heading">
                  <span className="micro-label">Linked investigations</span>
                </div>
                <article className="list-card list-card--soft">
                  <div className="button-row button-row--compact">
                    <button
                      className="text-button"
                      type="button"
                      onClick={() => onNavigate?.(`/attribution?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}
                    >
                      Open attribution
                    </button>
                    <button
                      className="text-button"
                      type="button"
                      onClick={() => onNavigate?.(`/lineage?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}
                    >
                      Highlight in lineage
                    </button>
                  </div>
                </article>
              </section>
            </>
          ) : (
            <div className="empty-state">
              <strong>No violation selected.</strong>
              <p className="muted-copy">Select a failure from the table to inspect its evidence, then jump into attribution or lineage for root-cause analysis.</p>
            </div>
          )}
        </aside>
      </div>
    </section>
  );
}

export default ViolationWorkbench;
