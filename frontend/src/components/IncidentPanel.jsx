import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { formatSystemList, getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function IncidentPanel({ incident, fallbackIncident }) {
  const hasActiveIncident = Boolean(incident?.violation_id);
  const display = hasActiveIncident ? incident : fallbackIncident;
  const tone = getStatusTone(display?.severity || incident?.severity);

  return (
    <section className="panel incident-panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Incident Panel</p>
          <h2>{hasActiveIncident ? "Current top issue" : "Last resolved incident"}</h2>
        </div>
        <span className={`badge badge--${tone}`}>
          {display?.severity || "PASS"}
        </span>
      </div>

      <article className={`incident-hero incident-hero--${tone}`}>
        <div>
          <span>{getSystemDisplayName(display?.week, { fallback: replaceSystemNames(display?.week) || "System wide" })}</span>
          <strong>{replaceSystemNames(display?.short_message || display?.title || display?.message || "No active incident")}</strong>
        </div>
        <div className="incident-meta">
          <span>Field {display?.field || "n/a"}</span>
          <span>{formatTimestamp(display?.detected_at || display?.time)}</span>
        </div>
      </article>

      <div className="detail-split">
        <article className="list-card">
          <strong>Downstream impact</strong>
          <p>
            {formatSystemList(display?.affected_systems, { empty: "No downstream systems are currently impacted." })}
          </p>
        </article>
        <article className="list-card">
          <strong>Recommended action</strong>
          <p>
            {display?.recommended_action
              || (hasActiveIncident
                ? "Inspect the failing contract, stabilize the producer output, and validate downstream consumers."
                : "Continue monitoring the next validation cycle.")}
          </p>
        </article>
      </div>

      <div className="incident-grid">
        <div>
          <span className="micro-label">Failing contract</span>
          <strong>{getSystemDisplayName(display?.contract_id || display?.source || display?.week, { fallback: replaceSystemNames(display?.contract_id || display?.source || display?.week) || "No active contract" })}</strong>
        </div>
        <div>
          <span className="micro-label">Failing field</span>
          <strong>{display?.field || "n/a"}</strong>
        </div>
        <div>
          <span className="micro-label">Detected time</span>
          <strong>{formatTimestamp(display?.detected_at || display?.time)}</strong>
        </div>
      </div>
    </section>
  );
}

export default IncidentPanel;
