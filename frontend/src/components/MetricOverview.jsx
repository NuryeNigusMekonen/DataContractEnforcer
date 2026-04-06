import { formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function MetricOverview({ items = [] }) {
  return (
    <section className="metric-grid">
      {items.map((item) => (
        <article className={`metric-card metric-card--${getStatusTone(item.tone || item.status)}`} key={item.label}>
          <div className="metric-card-top">
            <span>{item.label}</span>
            {item.status ? (
              <span className={`badge badge--${getStatusTone(item.status)}`}>
                {item.status}
              </span>
            ) : null}
          </div>
          <strong>{item.displayValue ?? formatCompactNumber(item.value)}</strong>
          <p>{item.context}</p>
          {item.meta ? <small>{item.meta}</small> : null}
        </article>
      ))}
    </section>
  );
}

export function buildOverviewItems({ kpi, summary, violations, aiRiskStatus }) {
  return [
    {
      label: "Contract Health Score",
      displayValue: `${kpi.health_score ?? summary.data_health_score ?? "--"}/100`,
      status: summary.fail ? "FAIL" : summary.warn ? "WARN" : "PASS",
      context: kpi.health_narrative || "Latest contract validation health across the end-to-end flow.",
      meta: `Passed ${formatCompactNumber(summary.pass)} of ${formatCompactNumber(summary.total_checks)} checks`,
    },
    {
      label: "Active Violations",
      displayValue: formatCompactNumber(violations.length),
      status: violations.length ? "FAIL" : "PASS",
      context: violations.length
        ? "Current contract failures with attached evidence are available below."
        : "No active contract violations in the current published run.",
    },
    {
      label: "High Risk Incidents",
      displayValue: formatCompactNumber(kpi.incident_count),
      status: kpi.incident_count ? "HIGH" : "PASS",
      context: kpi.incident_context || "No critical incidents are active.",
    },
    {
      label: "Last Validation Time",
      displayValue: formatTimestamp(kpi.last_validation_time),
      status: kpi.watcher?.status || "UNKNOWN",
      context: kpi.last_validation_context || "Most recent contract validation refresh.",
      meta: `Watcher ${kpi.watcher?.status || "unknown"}`,
    },
    {
      label: "AI Risk Status",
      displayValue: aiRiskStatus,
      status: aiRiskStatus,
      context: "Derived from embedding drift, prompt validation, and LLM output schema checks.",
    },
  ];
}

export default MetricOverview;
