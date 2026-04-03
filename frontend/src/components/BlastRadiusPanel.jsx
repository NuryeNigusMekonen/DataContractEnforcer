import { useState } from "react";

import { formatCompactNumber } from "../utils/formatters";

function BlastRadiusPanel({ blastRadius, isUpdated = false }) {
  const [expanded, setExpanded] = useState(false);
  const fields = expanded ? blastRadius.all_fields || [] : blastRadius.top_fields || [];

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Impact Analysis</p>
          <h2>Blast radius</h2>
        </div>
        <button className="text-button" type="button" onClick={() => setExpanded((current) => !current)}>
          {expanded ? "Collapse" : "Expand to full list"}
        </button>
      </div>

      <div className="stats-grid">
        <article className="stat-tile">
          <span>Affected systems</span>
          <strong>{blastRadius.affected_systems_count ?? 0}</strong>
        </article>
        <article className="stat-tile">
          <span>Max depth</span>
          <strong>{blastRadius.max_depth ?? 0}</strong>
        </article>
        <article className="stat-tile">
          <span>Impacted records</span>
          <strong>{formatCompactNumber(blastRadius.estimated_impacted_records ?? 0)}</strong>
        </article>
      </div>

      <div className="stack-list">
        {fields.map((field) => (
          <article className="list-card" key={field.field}>
            <div className="list-card-top">
              <strong>{field.field || "Unknown field"}</strong>
              <span>{formatCompactNumber(field.estimated_records ?? 0)} records</span>
            </div>
            <p>
              {field.affected_subscribers?.length ?? 0} systems affected across depth {field.contamination_depth ?? 0}
            </p>
          </article>
        ))}
        {!fields.length ? <p className="empty-copy">No downstream blast radius detected.</p> : null}
      </div>
    </section>
  );
}

export default BlastRadiusPanel;
