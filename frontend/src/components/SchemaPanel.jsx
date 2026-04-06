import { formatTimestamp } from "../utils/formatters";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function SchemaPanel({ schemaEvolution }) {
  const breakingChanges = (schemaEvolution.changes || []).filter(
    (change) => change.compatibility_verdict === "BREAKING",
  );

  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Schema Evolution</p>
          <h2>Compatibility watch</h2>
        </div>
        <span
          className={`status-pill ${
            schemaEvolution.compatibility_verdict === "BREAKING"
              ? "status-fail"
              : "status-pass"
          }`}
        >
          {schemaEvolution.compatibility_verdict || "UNKNOWN"}
        </span>
      </div>

      <p className="muted">
        {getSystemDisplayName(schemaEvolution.contract_id, { fallback: replaceSystemNames(schemaEvolution.contract_id) || "No contract" })} • Updated {formatTimestamp(schemaEvolution.last_updated)}
      </p>

      <div className="stack-list">
        {(breakingChanges.length ? breakingChanges : schemaEvolution.changes || []).slice(0, 6).map((change) => (
          <article className="mini-card" key={change.field_name}>
            <div className="card-heading">
              <strong>{change.field_name}</strong>
              <span
                className={`status-pill ${
                  change.compatibility_verdict === "BREAKING"
                    ? "status-fail"
                    : "status-pass"
                }`}
              >
                {change.compatibility_verdict}
              </span>
            </div>
            <p>{replaceSystemNames(change.rationale)}</p>
            <p className="muted">{replaceSystemNames(change.migration_recommendation)}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

export default SchemaPanel;
