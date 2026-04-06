import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { formatSystemList, getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function IncidentCard({ incident, isUpdated = false }) {
  const tone = getStatusTone(incident.severity);
  const isHealthyState =
    tone === "pass" &&
    !incident.week &&
    !incident.detected_at &&
    !(incident.affected_systems?.length);

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Current Risk</p>
          <h2>What needs attention now</h2>
        </div>
        <span className={`badge badge--${tone}`}>
          {incident.severity || "PASS"}
        </span>
      </div>

      {isHealthyState ? (
        <article className="incident-hero incident-hero--healthy">
          <span>All monitored flows are healthy</span>
          <strong>{replaceSystemNames(incident.short_message || incident.message || "No active incident is blocking the current run.")}</strong>
          <p className="muted-copy">{incident.recommended_action || "Continue monitoring live validations."}</p>
        </article>
      ) : (
        <>
          <article className="incident-hero">
            <span>{incident.field || "No active incident"}</span>
            <strong>{replaceSystemNames(incident.short_message || incident.message)}</strong>
          </article>

          <div className="detail-split">
            <article className="list-card">
              <strong>Affected systems</strong>
              <p>
                {formatSystemList(incident.affected_systems, { empty: "No downstream systems are currently impacted." })}
              </p>
            </article>
            <article className="list-card">
              <strong>Next step</strong>
              <p>{incident.recommended_action || "Continue monitoring."}</p>
            </article>
          </div>

          <div className="incident-details">
            <p className="muted-copy">System: {getSystemDisplayName(incident.week, { fallback: replaceSystemNames(incident.week) || "Unknown" })}</p>
            <p className="muted-copy">Detected: {formatTimestamp(incident.detected_at)}</p>
          </div>
        </>
      )}
    </section>
  );
}

export default IncidentCard;
