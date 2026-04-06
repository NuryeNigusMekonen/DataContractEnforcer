import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function compatibilityLabel(value) {
  return String(value || "UNKNOWN").replaceAll("_", " ");
}

function SchemaEvolutionPanel({ schemaEvolution }) {
  const changes = schemaEvolution.items || schemaEvolution.changes || [];
  const migrationPlan = schemaEvolution.migration_recommendation || [];

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Schema Evolution</p>
          <h2>Detected changes and migration guidance</h2>
        </div>
        <span className={`badge badge--${getStatusTone(schemaEvolution.compatibility_verdict)}`}>
          {compatibilityLabel(schemaEvolution.compatibility_verdict)}
        </span>
      </div>

      <p className="muted-copy">
        {getSystemDisplayName(schemaEvolution.contract_id, { fallback: replaceSystemNames(schemaEvolution.contract_id) || "No tracked contract" })} • Updated {formatTimestamp(schemaEvolution.last_updated)}
      </p>

      <div className="stack-list">
        {changes.slice(0, 8).map((change) => {
          const isBreaking = String(change.compatibility_verdict || change.compatibility || "").toUpperCase().includes("BREAK");
          return (
            <article className="list-card" key={`${change.contract_id}-${change.field_name}-${change.change}`}>
              <div className="list-card-top">
                <strong>{change.field_name || change.contract_name}</strong>
                <span className={`badge badge--${getStatusTone(change.compatibility_verdict || change.compatibility)}`}>
                  {compatibilityLabel(change.compatibility_verdict || change.compatibility)}
                </span>
              </div>
              <p>{replaceSystemNames(change.change_type || change.change || "Change detected")}</p>
              <p className="muted-copy">{replaceSystemNames(change.rationale || change.action_required)}</p>
              <p className="muted-copy">
                Migration required {isBreaking ? "Yes" : "No"} • Rollback plan {replaceSystemNames(change.action_required || migrationPlan[0] || "Review consumer compatibility before release.")}
              </p>
            </article>
          );
        })}
        {!changes.length ? <div className="empty-state"><strong>No schema evolution changes are currently recorded.</strong></div> : null}
      </div>
    </section>
  );
}

export default SchemaEvolutionPanel;
