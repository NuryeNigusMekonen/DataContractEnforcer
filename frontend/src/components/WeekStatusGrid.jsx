import { useState } from "react";

import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function WeekCard({ week, isUpdated }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <article className={`week-card ${isUpdated ? "week-card--updated" : ""}`}>
      <div className="week-card-top">
        <div>
          <h3>{week.week_name}</h3>
          <p>{week.checks_failed ?? 0} failed checks</p>
        </div>
        <span className={`badge badge--${getStatusTone(week.status)}`}>{week.status}</span>
      </div>
      <p className="muted-copy">Updated {formatTimestamp(week.last_updated)}</p>
      {expanded ? (
        <div className="week-card-details">
          <span>Passed: {week.checks_passed ?? 0}</span>
          <span>Warnings: {week.checks_warned ?? 0}</span>
          <span>Total checks: {week.total_checks ?? 0}</span>
        </div>
      ) : null}
      <button className="text-button text-button--left" type="button" onClick={() => setExpanded((current) => !current)}>
        {expanded ? "Hide details" : "Expand"}
      </button>
    </article>
  );
}

function WeekStatusGrid({ weeks, updatedWeekKeys = [] }) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">System Overview</p>
          <h2>Week status grid</h2>
        </div>
      </div>

      <div className="week-grid">
        {weeks.map((week) => (
          <WeekCard
            key={week.key}
            week={week}
            isUpdated={updatedWeekKeys.includes(week.key)}
          />
        ))}
      </div>
    </section>
  );
}

export default WeekStatusGrid;
