import { formatCompactNumber, formatTimeValue, formatTimestamp } from "../utils/formatters";

function KPICards({ kpi, isUpdated = false }) {
  const cards = [
    {
      label: "Contract Health",
      value: `${kpi.health_score ?? "--"}/100`,
      context: kpi.health_narrative || "Latest end-to-end validation health across the monitored flows.",
    },
    {
      label: "High-Risk Incidents",
      value: formatCompactNumber(kpi.incident_count),
      context: kpi.incident_context || "No critical incidents are active right now.",
    },
    {
      label: "Impacted Systems",
      value: formatCompactNumber(kpi.affected_systems_count),
      context: kpi.affected_systems_context || "Downstream impact is currently limited.",
    },
    {
      label: "Last Validation",
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
