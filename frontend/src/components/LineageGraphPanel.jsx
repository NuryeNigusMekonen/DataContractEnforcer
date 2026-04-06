import { useEffect, useMemo, useRef, useState } from "react";

import { replaceSystemNames, replaceWeekNames } from "../utils/systemNames";

const TYPE_COLORS = {
  SERVICE: "#1d4ed8",
  TABLE: "#047857",
  PIPELINE: "#7c3aed",
  FILE: "#b45309",
  EXTERNAL: "#6b7280",
  UNKNOWN: "#475569",
};
const MIN_ZOOM = 0.35;
const MAX_ZOOM = 12;
const DEFAULT_VIEWPORT_HEIGHT = 560;

function nodeColor(type) {
  return TYPE_COLORS[type] || TYPE_COLORS.UNKNOWN;
}

function edgeLabelColor(relationship) {
  if (relationship === "PRODUCES") {
    return "#1d4ed8";
  }
  if (relationship === "CONSUMES") {
    return "#047857";
  }
  return "#475569";
}

function computeLevels(nodes, edges) {
  const nodeIds = [...new Set(nodes.map((node) => node.id))].sort();
  const indegree = Object.fromEntries(nodeIds.map((id) => [id, 0]));
  const outgoing = Object.fromEntries(nodeIds.map((id) => [id, []]));

  [...edges]
    .sort((left, right) => {
      const leftKey = `${left.source}|${left.target}|${left.relationship}`;
      const rightKey = `${right.source}|${right.target}|${right.relationship}`;
      return leftKey.localeCompare(rightKey);
    })
    .forEach((edge) => {
      if (!outgoing[edge.source] || indegree[edge.target] === undefined) {
        return;
      }
      indegree[edge.target] += 1;
      outgoing[edge.source].push(edge.target);
    });
  Object.keys(outgoing).forEach((nodeId) => {
    outgoing[nodeId].sort((left, right) => left.localeCompare(right));
  });

  const levelById = {};
  const queue = [];
  Object.entries(indegree).forEach(([id, degree]) => {
    if (degree === 0) {
      queue.push(id);
      levelById[id] = 0;
    }
  });

  while (queue.length) {
    const current = queue.shift();
    const nextLevel = (levelById[current] ?? 0) + 1;
    outgoing[current].forEach((nextId) => {
      indegree[nextId] -= 1;
      if ((levelById[nextId] ?? -1) < nextLevel) {
        levelById[nextId] = nextLevel;
      }
      if (indegree[nextId] === 0) {
        queue.push(nextId);
      }
    });
  }

  nodeIds.forEach((id) => {
    if (levelById[id] === undefined) {
      levelById[id] = 0;
    }
  });
  return levelById;
}

function buildLinearLayout(nodes, edges) {
  if (!nodes.length) {
    return { svgWidth: 920, svgHeight: 320, placedNodes: [], placedEdges: [] };
  }
  const levels = computeLevels(nodes, edges);
  const maxLevel = Math.max(...Object.values(levels));
  const byLevel = {};
  nodes.forEach((node) => {
    const level = levels[node.id] ?? 0;
    if (!byLevel[level]) {
      byLevel[level] = [];
    }
    byLevel[level].push(node);
  });
  Object.values(byLevel).forEach((items) => {
    items.sort((left, right) => `${left.type}:${left.label}`.localeCompare(`${right.type}:${right.label}`));
  });

  const maxRows = Math.max(...Object.values(byLevel).map((items) => items.length));
  const svgWidth = Math.max(980, (maxLevel + 1) * 250 + 180);
  const svgHeight = Math.max(520, maxRows * 78 + 160);
  const laneWidth = (svgWidth - 150) / Math.max(1, maxLevel + 1);

  const nodeWidth = 124;
  const nodeHeight = 40;
  const placedNodes = [];
  const positionById = {};
  Object.entries(byLevel)
    .sort(([left], [right]) => Number(left) - Number(right))
    .forEach(([levelText, items]) => {
      const level = Number(levelText);
      const step = (svgHeight - 120) / (items.length + 1);
      const x = 75 + laneWidth * level;
      items.forEach((node, index) => {
        const y = 60 + step * (index + 1);
        const placed = { ...node, x, y, shape: "pill", w: nodeWidth, h: nodeHeight, hitRadius: 66 };
        placedNodes.push(placed);
        positionById[node.id] = placed;
      });
    });

  const placedEdges = edges
    .map((edge) => {
      const source = positionById[edge.source];
      const target = positionById[edge.target];
      if (!source || !target) {
        return null;
      }
      const sourceX = source.x + source.w / 2 - 4;
      const targetX = target.x - target.w / 2 + 4;
      const sameLane = Math.abs(source.x - target.x) < 24;
      const bend = sameLane ? 110 : Math.max(70, Math.abs(targetX - sourceX) * 0.34);
      const path = `M ${sourceX} ${source.y} C ${sourceX + bend} ${source.y}, ${targetX - bend} ${target.y}, ${targetX} ${target.y}`;
      return { ...edge, path, labelX: (sourceX + targetX) / 2, labelY: (source.y + target.y) / 2 };
    })
    .filter(Boolean)
    .sort((left, right) => edgeKey(left).localeCompare(edgeKey(right)));

  return { svgWidth, svgHeight, placedNodes, placedEdges };
}

function buildRadialLayout(nodes, edges) {
  if (!nodes.length) {
    return { svgWidth: 920, svgHeight: 320, placedNodes: [], placedEdges: [] };
  }

  const ringByType = {
    SERVICE: 0,
    TABLE: 1,
    PIPELINE: 2,
    FILE: 2,
    EXTERNAL: 3,
    UNKNOWN: 3,
  };

  const degreeById = {};
  nodes.forEach((node) => {
    degreeById[node.id] = 0;
  });
  edges.forEach((edge) => {
    if (degreeById[edge.source] !== undefined) {
      degreeById[edge.source] += 1;
    }
    if (degreeById[edge.target] !== undefined) {
      degreeById[edge.target] += 1;
    }
  });

  const byRing = {};
  nodes.forEach((node) => {
    const ring = ringByType[node.type] ?? 3;
    if (!byRing[ring]) {
      byRing[ring] = [];
    }
    byRing[ring].push(node);
  });

  Object.values(byRing).forEach((items) => {
    items.sort((left, right) => {
      const degreeDiff = (degreeById[right.id] || 0) - (degreeById[left.id] || 0);
      if (degreeDiff !== 0) {
        return degreeDiff;
      }
      return `${left.type}:${left.label}`.localeCompare(`${right.type}:${right.label}`);
    });
  });

  const ringIds = Object.keys(byRing)
    .map((value) => Number(value))
    .sort((left, right) => left - right);
  const maxRing = ringIds.length ? Math.max(...ringIds) : 0;
  const baseRadius = 118;
  const ringGap = 94;
  const estimatedMaxRadius = baseRadius + maxRing * ringGap + 120;
  const svgWidth = Math.max(980, estimatedMaxRadius * 2 + 170);
  const svgHeight = Math.max(620, estimatedMaxRadius * 2 + 100);
  const centerX = svgWidth / 2;
  const centerY = svgHeight / 2;

  const placedNodes = [];
  const positionById = {};
  ringIds.forEach((ring) => {
    const items = byRing[ring];
    const count = items.length;
    if (!count) {
      return;
    }
    let radius = baseRadius + ring * ringGap;
    const requiredCircumference = count * 28;
    const circumferenceRadius = requiredCircumference / (2 * Math.PI);
    radius = Math.max(radius, circumferenceRadius);
    if (ring === 0 && count === 1) {
      radius = 0;
    }
    const angleOffset = -Math.PI / 2 + ring * 0.18;
    items.forEach((node, index) => {
      const angle = angleOffset + (2 * Math.PI * index) / count;
      const x = centerX + radius * Math.cos(angle);
      const y = centerY + radius * Math.sin(angle);
      const placed = { ...node, x, y, shape: "dot", r: 6.5, hitRadius: 12 };
      placedNodes.push(placed);
      positionById[node.id] = placed;
    });
  });

  const placedEdges = edges
    .map((edge) => {
      const source = positionById[edge.source];
      const target = positionById[edge.target];
      if (!source || !target) {
        return null;
      }
      const sourceX = source.x;
      const sourceY = source.y;
      const targetX = target.x;
      const targetY = target.y;
      const midX = (sourceX + targetX) / 2;
      const midY = (sourceY + targetY) / 2;
      const controlX = midX + (centerX - midX) * 0.24;
      const controlY = midY + (centerY - midY) * 0.24;
      const path = `M ${sourceX} ${sourceY} Q ${controlX} ${controlY} ${targetX} ${targetY}`;
      const labelX = (sourceX + 2 * controlX + targetX) / 4;
      const labelY = (sourceY + 2 * controlY + targetY) / 4;
      return { ...edge, path, labelX, labelY };
    })
    .filter(Boolean)
    .sort((left, right) => {
      const leftKey = `${left.source}|${left.target}|${left.relationship}`;
      const rightKey = `${right.source}|${right.target}|${right.relationship}`;
      return leftKey.localeCompare(rightKey);
    });

  return { svgWidth, svgHeight, placedNodes, placedEdges };
}

function buildLayout(nodes, edges, layoutMode) {
  if (layoutMode === "linear") {
    return buildLinearLayout(nodes, edges);
  }
  return buildRadialLayout(nodes, edges);
}

function relationFilter(edges, relation) {
  if (relation === "ALL") {
    return edges;
  }
  return edges.filter((edge) => edge.relationship === relation);
}

function edgeKey(edge) {
  return `${edge.source}|${edge.target}|${edge.relationship}`;
}

function graphFingerprint(nodes, edges, layoutMode) {
  const nodePart = [...nodes]
    .map((node) => node.id)
    .sort((left, right) => left.localeCompare(right))
    .join(",");
  const edgePart = [...edges]
    .map(edgeKey)
    .sort((left, right) => left.localeCompare(right))
    .join(",");
  return `${layoutMode}::${nodePart}::${edgePart}`;
}

function traverse(seed, adjacency) {
  const seen = new Set([seed]);
  const queue = [seed];
  while (queue.length) {
    const current = queue.shift();
    (adjacency[current] || []).forEach((nextId) => {
      if (seen.has(nextId)) {
        return;
      }
      seen.add(nextId);
      queue.push(nextId);
    });
  }
  return seen;
}

function traversalHighlight(nodes, edges, selectedNodeId, direction) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const allEdges = new Set(edges.map(edgeKey));
  if (!selectedNodeId || !nodeIds.has(selectedNodeId)) {
    return {
      hasSelection: false,
      activeNodes: nodeIds,
      activeEdges: allEdges,
    };
  }

  const outgoing = Object.fromEntries([...nodeIds].map((id) => [id, []]));
  const incoming = Object.fromEntries([...nodeIds].map((id) => [id, []]));
  edges.forEach((edge) => {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
      return;
    }
    outgoing[edge.source].push(edge.target);
    incoming[edge.target].push(edge.source);
  });

  const downstream = traverse(selectedNodeId, outgoing);
  const upstream = traverse(selectedNodeId, incoming);

  let activeNodes;
  if (direction === "downstream") {
    activeNodes = new Set(downstream);
  } else if (direction === "upstream") {
    activeNodes = new Set(upstream);
  } else {
    activeNodes = new Set([...downstream, ...upstream]);
  }
  activeNodes.add(selectedNodeId);

  const activeEdges = new Set();
  edges.forEach((edge) => {
    if (!activeNodes.has(edge.source) || !activeNodes.has(edge.target)) {
      return;
    }
    if (direction === "downstream" && !downstream.has(edge.source)) {
      return;
    }
    if (direction === "upstream" && !upstream.has(edge.target)) {
      return;
    }
    activeEdges.add(edgeKey(edge));
  });

  return {
    hasSelection: true,
    activeNodes,
    activeEdges,
  };
}

function focusHighlight(nodes, edges, focusNodeIds) {
  const focusSet = new Set((focusNodeIds || []).filter(Boolean));
  if (!focusSet.size) {
    return {
      hasSelection: false,
      activeNodes: new Set(nodes.map((node) => node.id)),
      activeEdges: new Set(edges.map(edgeKey)),
    };
  }

  const activeNodes = new Set();
  nodes.forEach((node) => {
    if (focusSet.has(node.id)) {
      activeNodes.add(node.id);
    }
  });
  edges.forEach((edge) => {
    if (focusSet.has(edge.source) || focusSet.has(edge.target)) {
      activeNodes.add(edge.source);
      activeNodes.add(edge.target);
    }
  });
  const activeEdges = new Set(
    edges
      .filter((edge) => activeNodes.has(edge.source) && activeNodes.has(edge.target))
      .map(edgeKey),
  );

  return {
    hasSelection: true,
    activeNodes,
    activeEdges,
  };
}

function fullCoreSubgraph(nodes, edges, hops = 2) {
  const byId = Object.fromEntries(nodes.map((node) => [node.id, node]));
  const adjacency = Object.fromEntries(nodes.map((node) => [node.id, []]));
  edges.forEach((edge) => {
    if (!adjacency[edge.source] || !adjacency[edge.target]) {
      return;
    }
    adjacency[edge.source].push(edge.target);
    adjacency[edge.target].push(edge.source);
  });

  const isSeed = (node) => {
    const id = String(node.id || "");
    const path = String(node.path || "");
    return (
      id.startsWith("dataset::outputs/") ||
      id.startsWith("service::week") ||
      id.includes("week") ||
      path.includes("outputs/week") ||
      path.includes("contracts/")
    );
  };

  const seedIds = nodes.filter(isSeed).map((node) => node.id);
  const seen = new Set(seedIds);
  const depthById = Object.fromEntries(seedIds.map((id) => [id, 0]));
  const queue = [...seedIds];
  while (queue.length) {
    const current = queue.shift();
    const depth = depthById[current] ?? 0;
    if (depth >= hops) {
      continue;
    }
    (adjacency[current] || []).forEach((nextId) => {
      if (seen.has(nextId)) {
        return;
      }
      seen.add(nextId);
      depthById[nextId] = depth + 1;
      queue.push(nextId);
    });
  }

  const keepTypes = new Set(["SERVICE", "TABLE"]);
  const filteredNodes = nodes.filter((node) => {
    if (!seen.has(node.id)) {
      return false;
    }
    if (String(node.id).startsWith("service::week") || String(node.id).startsWith("dataset::outputs/")) {
      return true;
    }
    return keepTypes.has(String(node.type || "UNKNOWN"));
  });
  const keepIds = new Set(filteredNodes.map((node) => node.id));
  const filteredEdges = edges.filter((edge) => keepIds.has(edge.source) && keepIds.has(edge.target));
  return { nodes: filteredNodes, edges: filteredEdges };
}

function clamp(value, minValue, maxValue) {
  if (value < minValue) {
    return minValue;
  }
  if (value > maxValue) {
    return maxValue;
  }
  return value;
}

function boundsForNodes(nodes) {
  if (!nodes.length) {
    return null;
  }
  let minX = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;
  nodes.forEach((node) => {
    const radiusX = Number(node.w ? node.w / 2 : node.r || node.hitRadius || 16);
    const radiusY = Number(node.h ? node.h / 2 : node.r || node.hitRadius || 16);
    minX = Math.min(minX, node.x - radiusX);
    maxX = Math.max(maxX, node.x + radiusX);
    minY = Math.min(minY, node.y - radiusY);
    maxY = Math.max(maxY, node.y + radiusY);
  });
  return {
    minX,
    maxX,
    minY,
    maxY,
    width: maxX - minX,
    height: maxY - minY,
  };
}

function zoomForBounds(bounds, svgWidth, svgHeight) {
  if (!bounds) {
    return 1;
  }
  const paddedWidth = Math.max(240, bounds.width + 120);
  const paddedHeight = Math.max(220, bounds.height + 120);
  const zoomX = svgWidth / paddedWidth;
  const zoomY = svgHeight / paddedHeight;
  return clamp(Math.min(zoomX, zoomY) * 0.92, MIN_ZOOM, MAX_ZOOM);
}

function neighborhoodIds(nodeId, edges, hops = 1) {
  const adjacency = {};
  edges.forEach((edge) => {
    if (!adjacency[edge.source]) {
      adjacency[edge.source] = [];
    }
    if (!adjacency[edge.target]) {
      adjacency[edge.target] = [];
    }
    adjacency[edge.source].push(edge.target);
    adjacency[edge.target].push(edge.source);
  });

  const seen = new Set([nodeId]);
  const queue = [{ id: nodeId, depth: 0 }];
  while (queue.length) {
    const current = queue.shift();
    if (!current || current.depth >= hops) {
      continue;
    }
    (adjacency[current.id] || []).forEach((nextId) => {
      if (seen.has(nextId)) {
        return;
      }
      seen.add(nextId);
      queue.push({ id: nextId, depth: current.depth + 1 });
    });
  }
  return seen;
}

function LineageGraphPanel({
  lineageMap,
  isUpdated = false,
  viewportHeight = DEFAULT_VIEWPORT_HEIGHT,
  showHeader = true,
  showLegend = true,
  focusNodeIds = [],
  focusLabel = "",
  nameStyle = "system",
}) {
  const [view, setView] = useState("cross_week");
  const [fullScope, setFullScope] = useState("core");
  const [relationship, setRelationship] = useState("ALL");
  const [selectedNodeId, setSelectedNodeId] = useState("");
  const [selectedEdgeId, setSelectedEdgeId] = useState("");
  const [traversalDirection, setTraversalDirection] = useState("both");
  const [hideLabels, setHideLabels] = useState(false);
  const [zoom, setZoom] = useState(1);
  const [cameraCenter, setCameraCenter] = useState({ x: 0, y: 0 });
  const layoutCacheRef = useRef(new Map());
  const svgRef = useRef(null);
  const dragRef = useRef(null);
  const graph = view === "full" ? lineageMap?.full : lineageMap?.cross_week;
  const layoutMode = "linear";
  const zoomEnabled = view === "full";
  const formatGraphName = useMemo(
    () => (nameStyle === "week" ? replaceWeekNames : replaceSystemNames),
    [nameStyle],
  );

  const nodes = graph?.nodes || [];
  const edges = graph?.edges || [];

  const scoped = useMemo(() => {
    if (view !== "full" || fullScope === "all") {
      return { nodes, edges };
    }
    return fullCoreSubgraph(nodes, edges, 2);
  }, [view, fullScope, nodes, edges]);

  const visibleEdges = relationFilter(scoped.edges, relationship);
  const nodeIdsInEdges = new Set();
  visibleEdges.forEach((edge) => {
    nodeIdsInEdges.add(edge.source);
    nodeIdsInEdges.add(edge.target);
  });
  const visibleNodes =
    relationship === "ALL" ? scoped.nodes : scoped.nodes.filter((node) => nodeIdsInEdges.has(node.id));

  const layoutKey = useMemo(
    () => graphFingerprint(visibleNodes, visibleEdges, layoutMode),
    [visibleNodes, visibleEdges, layoutMode],
  );
  const { svgWidth, svgHeight, placedNodes, placedEdges } = useMemo(() => {
    const cache = layoutCacheRef.current;
    const cached = cache.get(layoutKey);
    if (cached) {
      return cached;
    }
    const fresh = buildLayout(visibleNodes, visibleEdges, layoutMode);
    cache.set(layoutKey, fresh);
    if (cache.size > 10) {
      const oldestKey = cache.keys().next().value;
      cache.delete(oldestKey);
    }
    return fresh;
  }, [layoutKey, visibleNodes, visibleEdges, layoutMode]);
  const highlight = useMemo(
    () => {
      if (selectedNodeId) {
        return traversalHighlight(visibleNodes, visibleEdges, selectedNodeId, traversalDirection);
      }
      return focusHighlight(visibleNodes, visibleEdges, focusNodeIds);
    },
    [focusNodeIds, selectedNodeId, traversalDirection, visibleEdges, visibleNodes],
  );
  const selectedNode = visibleNodes.find((node) => node.id === selectedNodeId) || null;
  const selectedEdge = placedEdges.find((edge) => edgeKey(edge) === selectedEdgeId) || null;
  const centered = useMemo(() => ({ x: svgWidth / 2, y: svgHeight / 2 }), [svgWidth, svgHeight]);
  const positionById = useMemo(
    () => Object.fromEntries(placedNodes.map((node) => [node.id, node])),
    [placedNodes],
  );
  const selectedIncomingCount = selectedNode
    ? visibleEdges.filter((edge) => edge.target === selectedNode.id).length
    : 0;
  const selectedOutgoingCount = selectedNode
    ? visibleEdges.filter((edge) => edge.source === selectedNode.id).length
    : 0;

  useEffect(() => {
    if (!selectedNodeId) {
      return;
    }
    const stillVisible = visibleNodes.some((node) => node.id === selectedNodeId);
    if (!stillVisible) {
      setSelectedNodeId("");
    }
  }, [selectedNodeId, visibleNodes]);
  useEffect(() => {
    if (selectedNodeId || selectedEdgeId) {
      return;
    }
    focusAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layoutKey, selectedNodeId, selectedEdgeId]);
  useEffect(() => {
    setHideLabels(false);
  }, [view]);

  const viewWidth = svgWidth / zoom;
  const viewHeight = svgHeight / zoom;
  const minCenterX = svgWidth <= viewWidth ? svgWidth / 2 : viewWidth / 2;
  const maxCenterX = svgWidth <= viewWidth ? svgWidth / 2 : svgWidth - viewWidth / 2;
  const minCenterY = svgHeight <= viewHeight ? svgHeight / 2 : viewHeight / 2;
  const maxCenterY = svgHeight <= viewHeight ? svgHeight / 2 : svgHeight - viewHeight / 2;
  const clampedCenterX = clamp(cameraCenter.x || centered.x, minCenterX, maxCenterX);
  const clampedCenterY = clamp(cameraCenter.y || centered.y, minCenterY, maxCenterY);
  const viewBox = `${clampedCenterX - viewWidth / 2} ${clampedCenterY - viewHeight / 2} ${viewWidth} ${viewHeight}`;

  function focusSelection(nodeId) {
    if (!nodeId) {
      return;
    }
    const localIds = neighborhoodIds(nodeId, visibleEdges, 1);
    const localNodes = Array.from(localIds)
      .map((id) => positionById[id])
      .filter(Boolean);
    if (!localNodes.length) {
      return;
    }
    const bounds = boundsForNodes(localNodes);
    if (!bounds) {
      return;
    }
    const minTarget = layoutMode === "radial" ? 2.4 : 1.8;
    const targetZoom = Math.max(minTarget, zoomForBounds(bounds, svgWidth, svgHeight));
    setCameraCenter({ x: (bounds.minX + bounds.maxX) / 2, y: (bounds.minY + bounds.maxY) / 2 });
    setZoom(clamp(targetZoom, MIN_ZOOM, MAX_ZOOM));
  }

  function focusEdge(edge) {
    if (!edge) {
      return;
    }
    const source = positionById[edge.source];
    const target = positionById[edge.target];
    if (!source || !target) {
      return;
    }
    const bounds = boundsForNodes([source, target]);
    if (!bounds) {
      return;
    }
    const minTarget = layoutMode === "radial" ? 2.8 : 2.2;
    const targetZoom = Math.max(minTarget, zoomForBounds(bounds, svgWidth, svgHeight));
    setCameraCenter({ x: (bounds.minX + bounds.maxX) / 2, y: (bounds.minY + bounds.maxY) / 2 });
    setZoom(clamp(targetZoom, MIN_ZOOM, MAX_ZOOM));
  }

  function focusAll() {
    if (!placedNodes.length) {
      return;
    }
    const bounds = boundsForNodes(placedNodes);
    if (!bounds) {
      return;
    }
    const baseZoom = zoomForBounds(bounds, svgWidth, svgHeight);
    const targetZoom = layoutMode === "radial" ? Math.max(1.08, baseZoom) : baseZoom;
    setCameraCenter({ x: (bounds.minX + bounds.maxX) / 2, y: (bounds.minY + bounds.maxY) / 2 });
    setZoom(clamp(targetZoom, MIN_ZOOM, MAX_ZOOM));
  }

  function pointerToGraph(event) {
    const svg = svgRef.current;
    if (!svg) {
      return null;
    }
    const rect = svg.getBoundingClientRect();
    if (!rect.width || !rect.height) {
      return null;
    }
    const ratioX = (event.clientX - rect.left) / rect.width;
    const ratioY = (event.clientY - rect.top) / rect.height;
    const x = clampedCenterX - viewWidth / 2 + ratioX * viewWidth;
    const y = clampedCenterY - viewHeight / 2 + ratioY * viewHeight;
    return { x, y, ratioX, ratioY, rect };
  }

  function handleWheel(event) {
    if (!zoomEnabled) {
      return;
    }
    event.preventDefault();
    const point = pointerToGraph(event);
    if (!point) {
      return;
    }
    const zoomFactor = event.deltaY < 0 ? 1.12 : 1 / 1.12;
    const nextZoom = clamp(zoom * zoomFactor, MIN_ZOOM, MAX_ZOOM);
    const nextViewWidth = svgWidth / nextZoom;
    const nextViewHeight = svgHeight / nextZoom;
    const nextLeft = point.x - point.ratioX * nextViewWidth;
    const nextTop = point.y - point.ratioY * nextViewHeight;
    setZoom(nextZoom);
    setCameraCenter({ x: nextLeft + nextViewWidth / 2, y: nextTop + nextViewHeight / 2 });
  }

  function handleMouseDown(event) {
    if (!zoomEnabled) {
      return;
    }
    const point = pointerToGraph(event);
    if (!point) {
      return;
    }
    dragRef.current = {
      startX: event.clientX,
      startY: event.clientY,
      startCenterX: clampedCenterX,
      startCenterY: clampedCenterY,
      rectWidth: point.rect.width,
      rectHeight: point.rect.height,
    };
  }

  function handleMouseMove(event) {
    const drag = dragRef.current;
    if (!drag) {
      return;
    }
    const dx = event.clientX - drag.startX;
    const dy = event.clientY - drag.startY;
    const graphDx = (dx / drag.rectWidth) * viewWidth;
    const graphDy = (dy / drag.rectHeight) * viewHeight;
    setCameraCenter({
      x: drag.startCenterX - graphDx,
      y: drag.startCenterY - graphDy,
    });
  }

  function stopDragging() {
    dragRef.current = null;
  }

  useEffect(() => {
    if (!selectedNodeId) {
      return;
    }
    if (!zoomEnabled) {
      return;
    }
    if (!positionById[selectedNodeId]) {
      return;
    }
    focusSelection(selectedNodeId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedNodeId, traversalDirection, zoomEnabled]);

  return (
    <section className={`panel ${isUpdated ? "panel--updated" : ""}`}>
      {showHeader ? (
        <div className="panel-heading">
          <div>
            <p className="section-kicker">Lineage Map</p>
            <h2>System dependency graph</h2>
            <p className="muted-copy">
              Captured {lineageMap?.captured_at || "Unknown"} • showing {visibleNodes.length} nodes • {visibleEdges.length} edges
              {view === "full" ? ` (from ${graph?.node_count ?? 0}/${graph?.edge_count ?? 0})` : ""}
            </p>
          </div>
        </div>
      ) : null}

      <div className="toolbar-row">
        <label className="field">
          <span>View</span>
          <select value={view} onChange={(event) => setView(event.target.value)}>
            <option value="cross_week">Cross-system contracts</option>
            <option value="full">Full merged lineage</option>
          </select>
        </label>
        <label className="field">
          <span>Relationship</span>
          <select value={relationship} onChange={(event) => setRelationship(event.target.value)}>
            <option value="ALL">All edges</option>
            <option value="CONSUMES">CONSUMES</option>
            <option value="PRODUCES">PRODUCES</option>
            <option value="WRITES">WRITES</option>
            <option value="CALLS">CALLS</option>
          </select>
        </label>
        <label className="field">
          <span>Traversal</span>
          <select value={traversalDirection} onChange={(event) => setTraversalDirection(event.target.value)}>
            <option value="both">Both directions</option>
            <option value="downstream">Downstream only</option>
            <option value="upstream">Upstream only</option>
          </select>
        </label>
        {view === "full" ? (
          <label className="field">
            <span>Full scope</span>
            <select value={fullScope} onChange={(event) => setFullScope(event.target.value)}>
              <option value="core">Core (recommended)</option>
              <option value="all">All nodes</option>
            </select>
          </label>
        ) : null}
      </div>
      <div className="lineage-selection-bar">
        {selectedNode ? (
          <>
            <p className="muted-copy">
              Selected <strong>{formatGraphName(selectedNode.label)}</strong> ({selectedNode.type}) • {traversalDirection} • in {selectedIncomingCount} • out {selectedOutgoingCount}
            </p>
            <button
              className="text-button text-button--left"
              type="button"
              onClick={() => {
                setSelectedNodeId("");
                setSelectedEdgeId("");
              }}
            >
              Clear selection
            </button>
          </>
        ) : selectedEdge ? (
          <>
            <p className="muted-copy">
              Selected edge <strong>{selectedEdge.relationship}</strong>: {formatGraphName(selectedEdge.source)} → {formatGraphName(selectedEdge.target)}
            </p>
            <button
              className="text-button text-button--left"
              type="button"
              onClick={() => {
                setSelectedNodeId("");
                setSelectedEdgeId("");
              }}
            >
              Clear selection
            </button>
          </>
        ) : focusLabel ? (
          <p className="muted-copy">{focusLabel}</p>
        ) : (
          <p className="muted-copy">
            {zoomEnabled
              ? "Click nodes/edges to zoom and focus. Use mouse wheel/trackpad to zoom, drag to pan."
              : "Click nodes and edges to inspect the cross-system dependency path. Zoom is disabled in this view to keep the full map stable."}
          </p>
        )}
      </div>

      {showLegend ? (
        <div className="lineage-legend">
          {Object.entries(TYPE_COLORS).map(([type, color]) => (
            <span className="legend-pill" key={type}>
              <i style={{ background: color }} />
              {type}
            </span>
          ))}
        </div>
      ) : null}

      {!placedNodes.length ? (
        <p className="empty-copy">No lineage graph data available for this view.</p>
      ) : (
        <div className="lineage-canvas-shell">
          <svg
            ref={svgRef}
            width="100%"
            height={viewportHeight}
            viewBox={viewBox}
            role="img"
            aria-label="Lineage graph"
            className={`lineage-svg ${layoutMode === "radial" ? "lineage-svg--radial" : ""} ${dragRef.current ? "lineage-svg--dragging" : ""}`}
            onWheel={zoomEnabled ? handleWheel : undefined}
            onMouseDown={zoomEnabled ? handleMouseDown : undefined}
            onMouseMove={handleMouseMove}
            onMouseUp={stopDragging}
            onMouseLeave={stopDragging}
            onClick={(event) => {
              if (event.target === svgRef.current) {
                setSelectedNodeId("");
                setSelectedEdgeId("");
              }
            }}
          >
            <defs>
              <marker id="lineage-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                <path d="M0,0 L8,4 L0,8 z" fill="#64748b" />
              </marker>
            </defs>
            {placedEdges.map((edge) => (
              <g key={`${edge.source}-${edge.target}-${edge.relationship}`}>
                <path
                  d={edge.path}
                  className={`lineage-edge ${
                    selectedEdgeId === edgeKey(edge)
                      ? "lineage-edge--selected"
                      : highlight.hasSelection && !highlight.activeEdges.has(edgeKey(edge))
                        ? "lineage-edge--dim"
                        : "lineage-edge--active"
                  }`}
                  stroke={edgeLabelColor(edge.relationship)}
                  markerEnd="url(#lineage-arrow)"
                  onClick={(event) => {
                    event.stopPropagation();
                    setSelectedNodeId("");
                    const next = selectedEdgeId === edgeKey(edge) ? "" : edgeKey(edge);
                    setSelectedEdgeId(next);
                    if (next && zoomEnabled) {
                      focusEdge(edge);
                    }
                  }}
                />
                <text
                  className={`lineage-edge-label ${
                    highlight.hasSelection && !highlight.activeEdges.has(edgeKey(edge)) ? "lineage-edge-label--dim" : ""
                  }`}
                  x={edge.labelX}
                  y={edge.labelY - 4}
                >
                  {hideLabels && !(selectedEdgeId === edgeKey(edge) || zoom >= 2.4) ? "" : edge.relationship}
                </text>
              </g>
            ))}
            {placedNodes.map((node) => (
              <g
                key={node.id}
                className={`lineage-node-group ${
                  highlight.hasSelection && !highlight.activeNodes.has(node.id) ? "lineage-node-group--dim" : "lineage-node-group--active"
                } ${selectedNodeId === node.id ? "lineage-node-group--selected" : ""}`}
                onClick={(event) => {
                  event.stopPropagation();
                  const next = selectedNodeId === node.id ? "" : node.id;
                  setSelectedNodeId(next);
                  setSelectedEdgeId("");
                  if (next && zoomEnabled) {
                    focusSelection(next);
                  }
                }}
                role="button"
                tabIndex={0}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    const next = selectedNodeId === node.id ? "" : node.id;
                    setSelectedNodeId(next);
                    if (next && zoomEnabled) {
                      focusSelection(next);
                    }
                  }
                }}
              >
                {node.shape === "dot" ? (
                  <>
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={node.r || 9}
                      className="lineage-node"
                      style={{
                        stroke: nodeColor(node.type),
                        fill: `${nodeColor(node.type)}22`,
                      }}
                    />
                    {!hideLabels && (selectedNodeId === node.id || zoom >= 2.4) ? (
                      <text className="lineage-node-label" x={node.x + 11} y={node.y - 10}>
                        {formatGraphName(node.label).length > 18 ? `${formatGraphName(node.label).slice(0, 18)}...` : formatGraphName(node.label)}
                      </text>
                    ) : null}
                  </>
                ) : (
                  <g transform={`translate(${node.x - (node.w || 124) / 2}, ${node.y - (node.h || 40) / 2})`}>
                    <rect
                      width={node.w || 124}
                      height={node.h || 40}
                      rx="11"
                      className="lineage-node"
                      style={{
                        stroke: nodeColor(node.type),
                        fill: `${nodeColor(node.type)}14`,
                      }}
                    />
                    {hideLabels ? null : (
                      <>
                        <text className="lineage-node-label" x="9" y="18">
                          {formatGraphName(node.label).length > 20 ? `${formatGraphName(node.label).slice(0, 20)}...` : formatGraphName(node.label)}
                        </text>
                        <text className="lineage-node-type" x="9" y="32">
                          {node.type}
                        </text>
                      </>
                    )}
                  </g>
                )}
                <title>{`${formatGraphName(node.id)}${node.path ? `\n${formatGraphName(node.path)}` : ""}`}</title>
              </g>
            ))}
          </svg>
        </div>
      )}

      {selectedNode ? (
        <div className="lineage-selection-bar">
          <p className="muted-copy">
            <strong>{formatGraphName(selectedNode.label)}</strong> • {selectedNode.type} • {formatGraphName(selectedNode.path || "No file path")} • {formatGraphName(selectedNode.purpose || "No metadata available")}
          </p>
        </div>
      ) : null}
    </section>
  );
}

export default LineageGraphPanel;
