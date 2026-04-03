import { formatCompactNumber, formatTimeValue, formatTimestamp } from "../utils/formatters";

function KPICards({ kpi, isUpdated = false }) {
  const cards = [
    {
      label: "Data Health Score",
      value: `${kpi.health_score ?? "--"}/100`,
      context: kpi.health_narrative || "Live validation health across monitored contracts.",
    },
    {
      label: "Active Critical Incidents",
      value: formatCompactNumber(kpi.incident_count),
      context: kpi.incident_context || "No critical incidents are active.",
    },
    {
      label: "Affected Systems Count",
      value: formatCompactNumber(kpi.affected_systems_count),
      context: kpi.affected_systems_context || "Downstream impact is currently limited.",
    },
    {
      label: "Last Validation Time",
      value: formatTimeValue(kpi.last_validation_time),
      context: formatTimestamp(kpi.last_validation_time),
    },
  ];

  return (
    <section className={`kpi-grid ${isUpdated ? "panel--updated" : ""}`}>
      {cards.map((card) => (
        <article className="kpi-card" key={card.label}>
          <strong>{card.value}</strong>
          <span>{card.label}</span>
          <p>{card.context}</p>
        </article>
      ))}
    </section>
  );
}

export default KPICards;
