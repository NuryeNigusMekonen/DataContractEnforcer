import { useEffect, useMemo, useState } from "react";

import BlameChainPanel from "../components/BlameChainPanel";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchAttributionPageData } from "../services/api";
import { computeFocusNodeIds } from "../utils/dashboardTransforms";
import { formatCompactNumber } from "../utils/formatters";
import { formatSystemList, getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function AttributionPage({ refreshToken, navigate, locationSearch }) {
  const { data, loading, error } = useCachedPageData(fetchAttributionPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 15000,
  });
  const [selectedViolationId, setSelectedViolationId] = useState("");
  const selectedFromLocation = useMemo(() => new URLSearchParams(locationSearch || "").get("violation") || "", [locationSearch]);

  useEffect(() => {
    const violations = data?.violations || [];
    if (!violations.length) {
      setSelectedViolationId("");
      return;
    }
    if (selectedFromLocation && violations.some((item) => item.violation_id === selectedFromLocation)) {
      setSelectedViolationId(selectedFromLocation);
      return;
    }
    if (!selectedViolationId || !violations.some((item) => item.violation_id === selectedViolationId)) {
      setSelectedViolationId(violations[0].violation_id);
    }
  }, [data?.violations, selectedFromLocation, selectedViolationId]);

  const selectedViolation = useMemo(
    () => (data?.violations || []).find((item) => item.violation_id === selectedViolationId) || null,
    [data?.violations, selectedViolationId],
  );
  const focusNodeIds = computeFocusNodeIds(selectedViolation, data?.lineageMap || {});
  const primaryBlame = useMemo(
    () => (data?.blame || []).find((item) => item.violation_id === selectedViolationId) || (data?.blame || [])[0] || null,
    [data?.blame, selectedViolationId],
  );

  function handleSelect(violationId) {
    setSelectedViolationId(violationId);
    navigate(`/attribution?violation=${encodeURIComponent(violationId)}`, { replace: true });
  }

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading attribution view…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel attribution-context-panel">
        <div className="toolbar-row">
          <label className="field field--grow">
            <span>Violation focus</span>
            <select value={selectedViolationId} onChange={(event) => handleSelect(event.target.value)}>
              {(data?.violations || []).map((violation) => (
                <option key={violation.violation_id} value={violation.violation_id}>
                  {getSystemDisplayName(violation.week, { short: true, fallback: replaceSystemNames(violation.week, { short: true }) || "Unknown" })} • {violation.field || "Unknown field"} • {violation.severity}
                </option>
              ))}
            </select>
          </label>
          <div className="button-row button-row--compact attribution-context-actions">
            <button
              className="text-button"
              type="button"
              onClick={() => selectedViolation && navigate(`/violations?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}
            >
              Open violation detail
            </button>
            <button
              className="text-button"
              type="button"
              onClick={() => selectedViolation && navigate(`/lineage?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}
            >
              Highlight in lineage
            </button>
          </div>
        </div>
      </section>

      <section className="dashboard-two-column dashboard-two-column--dense">
        <BlameChainPanel
          blame={data?.blame || []}
          selectedViolation={selectedViolation}
          blastRadius={data?.blastRadius || {}}
        />
        <div className="page-stack attribution-secondary-stack">
          <section className="panel">
            <div className="panel-heading">
              <div>
                <p className="section-kicker">Blast Radius</p>
                <h2>Downstream exposure and likely source</h2>
              </div>
            </div>

            <div className="detail-grid">
              <article className="detail-block">
                <span className="micro-label">Affected systems</span>
                <strong>{formatCompactNumber(data?.blastRadius?.affected_systems_count ?? 0)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Max depth</span>
                <strong>{formatCompactNumber(data?.blastRadius?.max_depth ?? 0)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Impacted fields</span>
                <strong>{formatCompactNumber(data?.blastRadius?.all_fields?.length ?? 0)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Likely commit</span>
                <strong>{primaryBlame?.commit_hash || "n/a"}</strong>
              </article>
            </div>

            <div className="detail-split">
              <article className="list-card">
                <strong>Likely file</strong>
                <p>{replaceSystemNames(primaryBlame?.file_path || "No file context available.")}</p>
              </article>
              <article className="list-card">
                <strong>Affected systems</strong>
                <p>{formatSystemList(selectedViolation?.affected_systems, { empty: "No downstream systems are currently attached to the selected issue." })}</p>
              </article>
            </div>
          </section>

          <section className="panel">
            <div className="panel-heading">
              <div>
                <p className="section-kicker">Lineage</p>
                <h2>Full traversal lives in the lineage workspace</h2>
              </div>
            </div>

            <p className="muted-copy">
              Attribution already shows likely source and blast radius. The full dependency graph, traversal controls, and cross-system contract inspection stay on the Lineage page so the same workspace is not duplicated here.
            </p>

            <div className="detail-grid">
              <article className="detail-block">
                <span className="micro-label">Highlighted nodes</span>
                <strong>{formatCompactNumber(focusNodeIds.length)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Graph nodes</span>
                <strong>{formatCompactNumber(data?.lineageMap?.cross_week?.nodes?.length ?? data?.lineageMap?.full?.nodes?.length ?? 0)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Graph edges</span>
                <strong>{formatCompactNumber(data?.lineageMap?.cross_week?.edges?.length ?? data?.lineageMap?.full?.edges?.length ?? 0)}</strong>
              </article>
              <article className="detail-block">
                <span className="micro-label">Focused field</span>
                <strong>{selectedViolation?.field || "n/a"}</strong>
              </article>
            </div>

            <div className="detail-split">
              <article className="list-card">
                <strong>Current handoff</strong>
                <p>
                  {selectedViolation
                    ? `Open Lineage to inspect the highlighted path for ${selectedViolation.field || "the selected field"} across related systems.`
                    : "Select a violation, then open Lineage to inspect the relevant dependency path."}
                </p>
              </article>
              <article className="list-card">
                <strong>Why it is separate</strong>
                <p>
                  Attribution stays focused on cause analysis. Lineage remains the single place for graph navigation, edge filtering, and traversal direction changes.
                </p>
              </article>
            </div>

            <div className="button-row button-row--compact">
              <button
                className="text-button"
                type="button"
                onClick={() => selectedViolation && navigate(`/lineage?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}
              >
                Open lineage workspace
              </button>
            </div>
          </section>
        </div>
      </section>
    </div>
  );
}

export default AttributionPage;
