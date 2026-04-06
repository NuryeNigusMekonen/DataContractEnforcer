import { useMemo, useState } from "react";

import useCachedPageData from "../hooks/useCachedPageData";
import { fetchSchemaEvolutionPageData } from "../services/api";
import { formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function normalizeSchemaNote(note) {
  if (!note) {
    return "";
  }
  if (typeof note === "string") {
    return note;
  }
  if (typeof note === "object") {
    return String(
      note.recommended_action
      || note.action
      || note.message
      || note.description
      || "",
    );
  }
  return String(note);
}

function joinSchemaNotes(notes) {
  const values = (Array.isArray(notes) ? notes : [notes])
    .map(normalizeSchemaNote)
    .map((note) => replaceSystemNames(note))
    .map((note) => note.trim())
    .filter(Boolean);
  return values.join(" • ");
}

function SchemaEvolutionPage({ refreshToken, navigate }) {
  const { data, loading, error } = useCachedPageData(fetchSchemaEvolutionPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 15000,
  });
  const [tab, setTab] = useState("changed");

  const items = data?.schemaEvolution?.items || data?.schemaEvolution?.changes || [];
  const grouped = useMemo(() => {
    const changed = [];
    const unchanged = [];
    items.forEach((item) => {
      const haystack = `${item.change || ""} ${item.rationale || ""}`.toLowerCase();
      if (haystack.includes("no material change")) {
        unchanged.push(item);
      } else {
        changed.push(item);
      }
    });
    return { changed, unchanged };
  }, [items]);

  const visible = tab === "changed" ? grouped.changed : grouped.unchanged;

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading schema evolution…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  const schema = data?.schemaEvolution || {};
  const rolloutNotes = schema.migration_checklist || schema.migration_recommendation || [];
  const rolloutNotesText = joinSchemaNotes(rolloutNotes);
  const rollbackNotesText = joinSchemaNotes(data?.schemaEvolution?.migration_recommendation || []);

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}
      <section className="panel">
        <div className="panel-heading">
          <div>
            <p className="section-kicker">Schema Evolution</p>
            <h2>{getSystemDisplayName(schema.contract_id, { fallback: replaceSystemNames(schema.contract_id) || "Schema watch" })}</h2>
            <p className="muted-copy">Updated {formatTimestamp(schema.last_updated)}</p>
          </div>
          <div className="page-inline-actions">
            <span className={`badge badge--${getStatusTone(schema.compatibility_verdict)}`}>
              {schema.compatibility_verdict || "UNKNOWN"}
            </span>
            <button className="text-button" type="button" onClick={() => navigate("/validation")}>
              Open validation
            </button>
            <button className="text-button" type="button" onClick={() => navigate("/artifacts")}>
              Open artifacts
            </button>
          </div>
        </div>

        <div className="detail-split">
          <article className="list-card">
            <strong>Migration requirements</strong>
            <p>{rolloutNotesText || "Review downstream compatibility before rollout."}</p>
          </article>
          <article className="list-card">
            <strong>Rollback plan</strong>
            <p>Revert the producer schema, restore the previous snapshot, and rerun validation before reopening consumers.</p>
          </article>
        </div>

        <div className="tab-row">
          <button className={`tab-chip ${tab === "changed" ? "tab-chip--active" : ""}`} type="button" onClick={() => setTab("changed")}>
            Changed fields ({grouped.changed.length})
          </button>
          <button className={`tab-chip ${tab === "unchanged" ? "tab-chip--active" : ""}`} type="button" onClick={() => setTab("unchanged")}>
            Unchanged fields ({grouped.unchanged.length})
          </button>
        </div>

        <div className="stack-list">
          {visible.map((item) => (
            <article className="list-card" key={`${item.contract_id}-${item.field_name}-${item.change}`}>
              <div className="list-card-top">
                <strong>{item.field_name || item.contract_name}</strong>
                <span className={`badge badge--${getStatusTone(item.compatibility_verdict || item.compatibility)}`}>
                  {item.compatibility_verdict || item.compatibility}
                </span>
              </div>
              <p>{replaceSystemNames(item.change || item.rationale)}</p>
              <p className="muted-copy">Migration required: {replaceSystemNames(item.action_required || "No immediate action required.")}</p>
              <p className="muted-copy">
                Rollback plan: {rollbackNotesText || "Review downstream consumers and revert the producer schema if necessary."}
              </p>
            </article>
          ))}
          {!visible.length ? (
            <div className="empty-state">
              <strong>No {tab} schema items are available.</strong>
              {tab === "changed" ? <p className="muted-copy">No material schema drift was detected in the latest comparison, so all tracked fields are currently unchanged.</p> : null}
            </div>
          ) : null}
        </div>
      </section>
    </div>
  );
}

export default SchemaEvolutionPage;
