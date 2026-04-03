import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function TimelinePanel({ timeline, expanded, onToggleExpanded, isUpdated = false }) {
  const items = timeline.items || [];

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Timeline</p>
          <h2>Recent events</h2>
        </div>
        <button className="text-button" type="button" onClick={onToggleExpanded}>
          {expanded ? "Show recent events" : "View full timeline"}
        </button>
      </div>

      <div className="timeline-list">
        {items.map((item) => (
          <article className="timeline-row" key={item.id}>
            <div className={`timeline-dot timeline-dot--${getStatusTone(item.severity)}`} />
            <div className="timeline-copy">
              <div className="timeline-copy-top">
                <strong>{formatTimestamp(item.time)}</strong>
                <span className={`badge badge--${getStatusTone(item.severity)}`}>
                  {item.severity || "UNKNOWN"}
                </span>
              </div>
              <p>{item.short_message || item.title}</p>
            </div>
          </article>
        ))}
        {!items.length ? <p className="empty-copy">No recent events were found.</p> : null}
      </div>
    </section>
  );
}

export default TimelinePanel;
