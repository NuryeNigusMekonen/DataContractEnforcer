import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function WeekCard({ week, isUpdated, onOpen }) {
  const actionable = typeof onOpen === "function";
  const displayName = getSystemDisplayName(week.week_name || week.contract_id, {
    short: true,
    fallback: replaceSystemNames(week.week_name || week.contract_id, { short: true }),
  });
  return (
    <article className={`week-card ${isUpdated ? "week-card--updated" : ""} ${actionable ? "week-card--interactive" : ""}`}>
      <div className="week-card-top">
        <div>
          <h3>{displayName}</h3>
          <p>{week.checks_failed ?? 0} failed checks</p>
        </div>
        <span className={`badge badge--${getStatusTone(week.status)}`}>{week.status}</span>
      </div>
      <p className="muted-copy">Updated {formatTimestamp(week.last_updated)}</p>
      <div className="week-card-details">
        <span>Passed: {week.checks_passed ?? 0}</span>
        <span>Warnings: {week.checks_warned ?? 0}</span>
        <span>Total checks: {week.total_checks ?? 0}</span>
      </div>
      {actionable ? (
        <button className="text-button text-button--left" type="button" onClick={onOpen}>
          {(week.checks_failed ?? 0) > 0 || ["FAIL", "ERROR"].includes(String(week.status || "").toUpperCase()) ? "Inspect violations" : "Review schema"
          }
        </button>
      ) : null}
    </article>
  );
}

function WeekStatusGrid({ weeks, updatedWeekKeys = [], onNavigate }) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Validation Runs</p>
          <h2>System contract status</h2>
        </div>
      </div>

      <div className="week-grid">
        {weeks.map((week) => (
          <WeekCard
            key={week.key}
            week={week}
            isUpdated={updatedWeekKeys.includes(week.key)}
            onOpen={onNavigate ? () => onNavigate((week.checks_failed ?? 0) > 0 || ["FAIL", "ERROR"].includes(String(week.status || "").toUpperCase()) ? "/violations" : "/schema-evolution") : undefined}
          />
        ))}
      </div>
    </section>
  );
}

export default WeekStatusGrid;
