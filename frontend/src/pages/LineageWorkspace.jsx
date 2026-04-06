import { useEffect, useState } from "react";

import LineageGraphPanel from "../components/LineageGraphPanel";
import { fetchLineageMap } from "../services/api";

const EMPTY_LINEAGE = {
  status: "missing",
  captured_at: null,
  last_updated: null,
  full: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
  cross_week: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
};

function LineageWorkspace() {
  const [lineageMap, setLineageMap] = useState(EMPTY_LINEAGE);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastRefresh, setLastRefresh] = useState("");

  async function loadLineage() {
    try {
      setLoading(true);
      const payload = await fetchLineageMap();
      setLineageMap({ ...EMPTY_LINEAGE, ...payload });
      setError("");
      setLastRefresh(new Date().toLocaleTimeString());
    } catch (requestError) {
      setError(requestError.message || "Failed to load lineage map.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    document.body.classList.add("lineage-workspace-body");
    return () => {
      document.body.classList.remove("lineage-workspace-body");
    };
  }, []);

  useEffect(() => {
    loadLineage();
  }, []);

  return (
    <main className="lineage-workspace-shell">
      <header className="lineage-workspace-header">
        <p className="section-kicker">Lineage Workspace</p>
        <div className="toolbar-row toolbar-row--compact">
          <p className="muted-copy">Last refresh {lastRefresh || "Unavailable"}</p>
          <a className="primary-button primary-button--ghost" href="/">
            Back to dashboard
          </a>
          <button className="primary-button" type="button" onClick={loadLineage} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh lineage"}
          </button>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="lineage-workspace-main">
        <LineageGraphPanel lineageMap={lineageMap} viewportHeight={720} showHeader={false} showLegend={false} />
      </section>
    </main>
  );
}

export default LineageWorkspace;
