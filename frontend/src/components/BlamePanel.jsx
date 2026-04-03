import { formatTimestamp } from "../utils/formatters";

function BlamePanel({ blame, expanded, onToggleExpanded, isUpdated = false }) {
  const items = blame.items || [];

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Impact Analysis</p>
          <h2>Blame chain</h2>
        </div>
        <button className="text-button" type="button" onClick={onToggleExpanded}>
          {expanded ? "Show top 3" : "Expand to full list"}
        </button>
      </div>

      <div className="stack-list">
        {items.map((entry) => (
          <article className="list-card" key={`${entry.file_path}-${entry.commit_hash}`}>
            <div className="list-card-top">
              <strong>{entry.file || entry.file_path}</strong>
              <span>{entry.confidence}% confidence</span>
            </div>
            <p>{entry.author || "Unknown author"} • {entry.commit || "n/a"}</p>
            <p className="muted-copy">{formatTimestamp(entry.commit_timestamp)}</p>
          </article>
        ))}
        {!items.length ? <p className="empty-copy">No blame candidates are available.</p> : null}
      </div>
    </section>
  );
}

export default BlamePanel;
