import { formatTimestamp } from "../utils/formatters";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

const STATUS_CLASS = {
  PASS: "status-pass",
  WARN: "status-warn",
  FAIL: "status-fail",
  ERROR: "status-fail",
};

const CARD_CLASS = {
  PASS: "week-card--pass",
  WARN: "week-card--warn",
  FAIL: "week-card--fail",
  ERROR: "week-card--fail",
};

function WeekStatusCard({ week, isUpdated = false }) {
  const statusClass = STATUS_CLASS[week.status] || "status-neutral";
  const cardClass = CARD_CLASS[week.status] || "week-card--neutral";
  const displayName = getSystemDisplayName(week.week_name || week.contract_id, {
    short: true,
    fallback: replaceSystemNames(week.week_name || week.contract_id, { short: true }),
  });

  return (
    <article className={`panel week-card ${cardClass} ${isUpdated ? "week-card--updated" : ""}`}>
      <div className="week-card-top">
        <div>
          <p className="eyebrow">{displayName}</p>
          <h3>{week.total_checks} checks</h3>
        </div>
        <span className={`status-pill ${statusClass}`}>{week.status}</span>
      </div>
      <div className="week-metrics">
        <div>
          <span>Passed</span>
          <strong>{week.checks_passed}</strong>
        </div>
        <div>
          <span>Failed</span>
          <strong>{week.checks_failed}</strong>
        </div>
        <div>
          <span>Warned</span>
          <strong>{week.checks_warned}</strong>
        </div>
      </div>
      <p className="muted">
        Updated {formatTimestamp(week.last_updated)}
      </p>
    </article>
  );
}

export default WeekStatusCard;
