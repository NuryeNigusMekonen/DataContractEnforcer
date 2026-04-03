function SummaryCard({ title, value, subtitle, accent = "blue" }) {
  return (
    <article className={`panel metric-card metric-card--${accent}`}>
      <p className="eyebrow">{title}</p>
      <h3>{value}</h3>
      <p className="muted">{subtitle}</p>
    </article>
  );
}

export default SummaryCard;
