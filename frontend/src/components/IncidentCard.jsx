import { useState } from "react";

import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function IncidentCard({ incident, isUpdated = false }) {
  const [showDetails, setShowDetails] = useState(false);

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Incident And Simulation</p>
          <h2>Current incident</h2>
        </div>
        <span className={`badge badge--${getStatusTone(incident.severity)}`}>
          {incident.severity || "PASS"}
        </span>
      </div>

      <article className="incident-hero">
        <span>{incident.field || "No active incident"}</span>
        <strong>{incident.short_message || incident.message}</strong>
      </article>

      <div className="detail-split">
        <article className="list-card">
          <strong>Affected systems</strong>
          <p>
            {incident.affected_systems?.length
              ? incident.affected_systems.join(", ")
              : "No downstream systems are currently impacted."}
          </p>
        </article>
        <article className="list-card">
          <strong>Recommended action</strong>
          <p>{incident.recommended_action || "Continue monitoring."}</p>
        </article>
      </div>

      {showDetails ? (
        <div className="incident-details">
          <p className="muted-copy">Week: {incident.week || "Unknown"}</p>
          <p className="muted-copy">Detected: {formatTimestamp(incident.detected_at)}</p>
        </div>
      ) : null}

      <button className="primary-button primary-button--ghost" type="button" onClick={() => setShowDetails((current) => !current)}>
        {showDetails ? "Hide details" : "View details"}
      </button>
    </section>
  );
}

export default IncidentCard;
