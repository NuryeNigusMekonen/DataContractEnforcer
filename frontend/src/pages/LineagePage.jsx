import { useEffect, useMemo, useState } from "react";

import LineageGraphPanel from "../components/LineageGraphPanel";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchLineagePageData } from "../services/api";
import { computeFocusNodeIds } from "../utils/dashboardTransforms";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function violationSelectionKey(violation) {
  if (!violation) {
    return "";
  }
  return [
    violation.week,
    violation.check_id,
    violation.field,
    violation.severity,
    violation.message,
  ].map((part) => String(part || "").trim().toLowerCase()).join("|");
}

function computeLineageViewportHeight() {
  if (typeof window === "undefined") {
    return 560;
  }
  return Math.max(460, Math.min(640, window.innerHeight - 320));
}

function filterGraph(graph, search, nodeType) {
  const query = search.trim().toLowerCase();
  const nodes = (graph?.nodes || []).filter((node) => {
    if (nodeType !== "ALL" && String(node.type || "UNKNOWN") !== nodeType) {
      return false;
    }
    if (!query) {
      return true;
    }
    return `${node.id} ${node.label} ${node.path} ${node.purpose} ${replaceSystemNames(node.id)} ${replaceSystemNames(node.label)} ${replaceSystemNames(node.path)} ${replaceSystemNames(node.purpose)}`.toLowerCase().includes(query);
  });
  const keepIds = new Set(nodes.map((node) => node.id));
  const edges = (graph?.edges || []).filter((edge) => keepIds.has(edge.source) && keepIds.has(edge.target));
  return {
    ...graph,
    nodes,
    edges,
    node_count: nodes.length,
    edge_count: edges.length,
  };
}

function LineagePage({ refreshToken, navigate, locationSearch }) {
  const { data, loading, error } = useCachedPageData(fetchLineagePageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 15000,
  });
  const [search, setSearch] = useState("");
  const [nodeType, setNodeType] = useState("ALL");
  const [selectedViolationId, setSelectedViolationId] = useState(() => {
    const fromLocation = new URLSearchParams(locationSearch || "").get("violation") || "";
    if (fromLocation) {
      return fromLocation;
    }
    return window.localStorage.getItem("dashboard-lineage-selected-violation") || "";
  });
  const [selectedViolationKey, setSelectedViolationKey] = useState(() => window.localStorage.getItem("dashboard-lineage-selected-violation-key") || "");
  const [viewportHeight, setViewportHeight] = useState(computeLineageViewportHeight);

  const selectedViolation = useMemo(
    () => (data?.violations || []).find((item) => item.violation_id === selectedViolationId) || null,
    [data?.violations, selectedViolationId],
  );

  const filteredLineageMap = useMemo(() => {
    if (!data?.lineageMap) {
      return {
        status: "missing",
        captured_at: null,
        last_updated: null,
        full: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
        cross_week: { nodes: [], edges: [], node_count: 0, edge_count: 0 },
      };
    }
    return {
      ...data.lineageMap,
      full: filterGraph(data.lineageMap.full, search, nodeType),
      cross_week: filterGraph(data.lineageMap.cross_week, search, nodeType),
    };
  }, [data?.lineageMap, nodeType, search]);

  const nodeTypes = useMemo(() => {
    const set = new Set((data?.lineageMap?.full?.nodes || []).map((node) => String(node.type || "UNKNOWN")));
    return ["ALL", ...Array.from(set).sort()];
  }, [data?.lineageMap]);

  useEffect(() => {
    const nextViolationId = new URLSearchParams(locationSearch || "").get("violation") || "";
    if (nextViolationId !== selectedViolationId) {
      setSelectedViolationId(nextViolationId);
    }
  }, [locationSearch, selectedViolationId]);

  useEffect(() => {
    const violations = data?.violations || [];
    if (!violations.length || !selectedViolationId) {
      return;
    }

    const current = violations.find((item) => item.violation_id === selectedViolationId);
    if (current) {
      const key = violationSelectionKey(current);
      if (key && key !== selectedViolationKey) {
        setSelectedViolationKey(key);
      }
      window.localStorage.setItem("dashboard-lineage-selected-violation", current.violation_id || "");
      window.localStorage.setItem("dashboard-lineage-selected-violation-key", key);
      return;
    }

    if (selectedViolationKey) {
      const remapped = violations.find((item) => violationSelectionKey(item) === selectedViolationKey);
      if (remapped?.violation_id) {
        setSelectedViolationId(remapped.violation_id);
        navigate(`/lineage?violation=${encodeURIComponent(remapped.violation_id)}`, { replace: true });
      }
    }
  }, [data?.violations, navigate, selectedViolationId, selectedViolationKey]);

  useEffect(() => {
    function handleResize() {
      setViewportHeight(computeLineageViewportHeight());
    }

    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  function handleViolationFocus(nextViolationId) {
    setSelectedViolationId(nextViolationId);
    if (!nextViolationId) {
      setSelectedViolationKey("");
      window.localStorage.removeItem("dashboard-lineage-selected-violation");
      window.localStorage.removeItem("dashboard-lineage-selected-violation-key");
      navigate("/lineage", { replace: true });
      return;
    }
    const selected = (data?.violations || []).find((item) => item.violation_id === nextViolationId);
    const key = violationSelectionKey(selected);
    setSelectedViolationKey(key);
    window.localStorage.setItem("dashboard-lineage-selected-violation", nextViolationId);
    window.localStorage.setItem("dashboard-lineage-selected-violation-key", key);
    navigate(`/lineage?violation=${encodeURIComponent(nextViolationId)}`, { replace: true });
  }

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading lineage map…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  return (
    <div className="page-stack page-stack--lineage">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel page-toolbar page-toolbar--lineage">
        <div className="toolbar-row">
          <label className="field field--grow">
            <span>Search graph</span>
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Node id, label, path, or purpose"
            />
          </label>
          <label className="field">
            <span>Node type</span>
            <select value={nodeType} onChange={(event) => setNodeType(event.target.value)}>
              {nodeTypes.map((type) => (
                <option key={type} value={type}>{type}</option>
              ))}
            </select>
          </label>
          <label className="field field--grow">
            <span>Highlight issue path</span>
            <select value={selectedViolationId} onChange={(event) => handleViolationFocus(event.target.value)}>
              <option value="">No issue selected</option>
              {(data?.violations || []).map((violation) => (
                <option key={violation.violation_id} value={violation.violation_id}>
                  {getSystemDisplayName(violation.week, { short: true, fallback: replaceSystemNames(violation.week, { short: true }) || "Unknown" })} • {violation.field || "Unknown field"} • {violation.severity}
                </option>
              ))}
            </select>
          </label>
          <div className="button-row button-row--compact lineage-toolbar-links">
            <button className="text-button" type="button" onClick={() => navigate("/violations")}>
              Open violations
            </button>
            {selectedViolation ? (
              <button className="text-button" type="button" onClick={() => navigate(`/attribution?violation=${encodeURIComponent(selectedViolation.violation_id)}`)}>
                Open attribution
              </button>
            ) : null}
          </div>
        </div>
      </section>

      <LineageGraphPanel
        lineageMap={filteredLineageMap}
        viewportHeight={viewportHeight}
        showHeader={false}
        nameStyle="week"
        focusNodeIds={computeFocusNodeIds(selectedViolation, data?.lineageMap || {})}
        focusLabel={
          selectedViolation
            ? `Highlighted lineage path for ${selectedViolation.field || "selected issue"}.`
            : "Search and filter the graph, or choose an issue to highlight its likely path."
        }
      />
    </div>
  );
}

export default LineagePage;
