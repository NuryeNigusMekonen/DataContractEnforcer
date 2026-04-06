import { getStatusTone } from "../utils/status";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function ViolationsTable({
  violations,
  severityFilter,
  onSeverityChange,
  isUpdated = false,
}) {
  const emptyMessage =
    severityFilter === "ALL"
      ? "No active contract violations in the latest run."
      : `No ${severityFilter.toLowerCase()} severity violations in the latest run.`;

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Issues</p>
          <h2>Top contract violations</h2>
        </div>
      </div>

      <div className="toolbar-row">
        <label className="field">
          <span>Severity</span>
          <select value={severityFilter} onChange={(event) => onSeverityChange(event.target.value)}>
            <option value="ALL">All severities</option>
            <option value="HIGH">High</option>
            <option value="MEDIUM">Medium</option>
            <option value="LOW">Low</option>
          </select>
        </label>
      </div>

      {violations.length ? (
        <div className="table-shell">
          <table className="data-table">
            <thead>
              <tr>
                <th>Field</th>
                <th>Severity</th>
                <th>Week</th>
                <th>Short message</th>
              </tr>
            </thead>
            <tbody>
              {violations.map((violation) => (
                <tr key={violation.violation_id}>
                  <td>{violation.field || "Unknown field"}</td>
                  <td>
                    <span className={`badge badge--${getStatusTone(violation.severity)}`}>
                      {violation.severity || "UNKNOWN"}
                    </span>
                  </td>
                  <td>{getSystemDisplayName(violation.week, { short: true, fallback: replaceSystemNames(violation.week, { short: true }) || "Unknown" })}</td>
                  <td>{replaceSystemNames(violation.short_message || violation.message)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="empty-state">
          <strong>{emptyMessage}</strong>
          <p className="muted-copy">
            {severityFilter === "ALL"
              ? "The latest validation run did not produce any contract failures."
              : "Try a broader severity filter if you want to inspect lower-priority findings."}
          </p>
        </div>
      )}
    </section>
  );
}

export default ViolationsTable;
