import { useMemo } from "react";

import { getStatusTone } from "../utils/status";

function ViolationsTable({
  violations,
  severityFilter,
  search,
  onSeverityChange,
  onSearchChange,
  onToggleExpanded,
  expanded,
  isUpdated = false,
}) {
  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) {
      return violations;
    }
    return violations.filter((violation) =>
      [violation.field, violation.week, violation.short_message]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query)),
    );
  }, [search, violations]);

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Issues</p>
          <h2>Violations requiring operator attention</h2>
        </div>
        <button className="text-button" type="button" onClick={onToggleExpanded}>
          {expanded ? "Show top 10" : "View all"}
        </button>
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
        <label className="field field--grow">
          <span>Search</span>
          <input
            type="search"
            value={search}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Search field, week, or message"
          />
        </label>
      </div>

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
            {filteredRows.length ? (
              filteredRows.map((violation) => (
                <tr key={violation.violation_id}>
                  <td>{violation.field || "Unknown field"}</td>
                  <td>
                    <span className={`badge badge--${getStatusTone(violation.severity)}`}>
                      {violation.severity || "UNKNOWN"}
                    </span>
                  </td>
                  <td>{violation.week || "Unknown"}</td>
                  <td>{violation.short_message || violation.message}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="4" className="table-empty">
                  No violations match the current filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default ViolationsTable;
