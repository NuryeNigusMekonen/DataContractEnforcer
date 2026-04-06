import { formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { formatSystemList, replaceSystemNames } from "../utils/systemNames";

function matchingEntries(blame, selectedViolation) {
  if (!selectedViolation) {
    return blame.slice(0, 6);
  }
  const field = String(selectedViolation.field || "").toLowerCase();
  const violationId = selectedViolation.violation_id;
  const prioritized = blame.filter((entry) => {
    const fields = (entry.affected_fields || []).map((item) => String(item).toLowerCase());
    return entry.violation_id === violationId || fields.includes(field);
  });
  return prioritized.length ? prioritized : blame.slice(0, 6);
}

function BlameChainPanel({ blame = [], selectedViolation, blastRadius }) {
  const visible = matchingEntries(blame, selectedViolation);
  const impact = selectedViolation
    ? (blastRadius.all_fields || []).find((entry) => entry.field === selectedViolation.field)
    : null;

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Blame Chain</p>
          <h2>Likely source of the failing change</h2>
        </div>
      </div>

      <div className="detail-split">
        <article className="list-card">
          <strong>Lineage traversal result</strong>
          <p>
            {impact
              ? `${formatSystemList(impact.affected_subscribers, { empty: "No downstream systems" })} across depth ${impact.contamination_depth ?? 0}.`
              : "Select a violation to inspect its likely blast radius and affected path."}
          </p>
        </article>
        <article className="list-card">
          <strong>Affected downstream systems</strong>
          <p>
            {formatSystemList(selectedViolation?.affected_systems, { empty: "No downstream systems are currently attached to the selected issue." })}
          </p>
        </article>
      </div>

      <div className="stack-list">
        {visible.map((entry) => (
          <article className="list-card blame-card" key={`${entry.file_path}-${entry.commit_hash}`}>
            <div className="list-card-top">
              <strong>{replaceSystemNames(entry.file_path)}</strong>
              <span className={`badge badge--${getStatusTone(entry.severity)}`}>
                {Math.round((entry.confidence_score || 0) * 100)}% confidence
              </span>
            </div>
            <p>{entry.author || "Unknown author"} • {entry.commit_hash || "n/a"}</p>
            <p className="muted-copy">{formatTimestamp(entry.commit_timestamp)}</p>
            <p className="muted-copy">{entry.message || "No commit message captured."}</p>
            <p className="muted-copy">
              Impacted fields {formatCompactNumber(entry.impact_count)} • {entry.affected_fields?.join(", ") || "n/a"}
            </p>
          </article>
        ))}
        {!visible.length ? <div className="empty-state"><strong>No blame chain data is available.</strong></div> : null}
      </div>
    </section>
  );
}

export default BlameChainPanel;
