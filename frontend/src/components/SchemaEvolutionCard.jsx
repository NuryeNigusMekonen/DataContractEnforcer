import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function SchemaEvolutionCard({ schemaEvolution, isUpdated = false }) {
  const items = (schemaEvolution.items || []).slice(0, 6);

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">System Overview</p>
          <h2>Schema evolution</h2>
        </div>
        <span className={`badge badge--${getStatusTone(schemaEvolution.compatibility_verdict)}`}>
          {schemaEvolution.compatibility_verdict || "UNKNOWN"}
        </span>
      </div>

      <p className="muted-copy">Updated {formatTimestamp(schemaEvolution.last_updated)}</p>

      <div className="stack-list">
        {items.map((item) => (
          <article className="list-card" key={`${item.contract_id}-${item.field_name}`}>
            <div className="list-card-top">
              <strong>{item.contract_name}</strong>
              <span className={`badge badge--${getStatusTone(item.compatibility)}`}>
                {item.compatibility}
              </span>
            </div>
            <p>{item.field_name} • {item.change}</p>
            <p className="muted-copy">{item.action_required}</p>
          </article>
        ))}
        {!items.length ? <p className="empty-copy">No schema evolution data is available.</p> : null}
      </div>
    </section>
  );
}

export default SchemaEvolutionCard;
