import { getStatusTone } from "../utils/status";

function BoolCell({ value, label }) {
  return (
    <span className={`badge badge--${value ? "pass" : "neutral"}`}>
      {value ? label || "Active" : "Inactive"}
    </span>
  );
}

function CoverageMatrix({ rows = [] }) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Contract Coverage Matrix</p>
          <h2>Validation and attribution coverage by contract</h2>
        </div>
      </div>

      <div className="table-shell">
        <table className="data-table data-table--matrix">
          <thead>
            <tr>
              <th>Upstream system</th>
              <th>Downstream system</th>
              <th>Contract name</th>
              <th>Validation</th>
              <th>Attribution</th>
              <th>Schema tracking</th>
              <th>AI extensions</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.contractId}>
                <td>{row.upstreamSystem}</td>
                <td>{row.downstreamSystem}</td>
                <td>
                  <strong>{row.contractName}</strong>
                  <div className="table-subcopy">{row.dataset}</div>
                </td>
                <td><BoolCell value={row.validationActive} /></td>
                <td><BoolCell value={row.attributionActive} /></td>
                <td><BoolCell value={row.schemaEvolutionTracking} label="Tracked" /></td>
                <td><BoolCell value={row.aiExtensionsApplied} label="Applied" /></td>
                <td>
                  <span className={`badge badge--${getStatusTone(row.status)}`}>{row.status}</span>
                </td>
              </tr>
            ))}
            {!rows.length ? (
              <tr>
                <td colSpan="8" className="table-empty">No contract coverage data is available.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default CoverageMatrix;
