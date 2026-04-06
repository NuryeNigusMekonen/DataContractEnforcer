import { useMemo, useState } from "react";

import { formatCompactNumber } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

const SERIES = [
  { key: "passed", label: "Passed", color: "#34d399" },
  { key: "failed", label: "Failed", color: "#f87171" },
  { key: "warned", label: "Warnings", color: "#fbbf24" },
];

const CHART_DIMENSIONS = {
  width: 560,
  height: 190,
  marginTop: 12,
  marginRight: 64,
  marginBottom: 58,
  marginLeft: 16,
  tooltipWidth: 176,
  tooltipInset: 10,
};

function plotWidth() {
  return CHART_DIMENSIONS.width - CHART_DIMENSIONS.marginLeft - CHART_DIMENSIONS.marginRight;
}

function anchorX(index, itemsLength) {
  const width = plotWidth();
  if (!itemsLength) {
    return CHART_DIMENSIONS.marginLeft;
  }
  return itemsLength === 1
    ? CHART_DIMENSIONS.marginLeft + width / 2
    : CHART_DIMENSIONS.marginLeft + (index / (itemsLength - 1)) * width;
}

function compactAxisLabelLines(label) {
  const text = String(label || "").trim();
  const normalized = text.toLowerCase();
  if (normalized.includes("intent")) {
    return ["Intent"];
  }
  if (normalized.includes("governance")) {
    return ["Governance"];
  }
  if (normalized.includes("document intelligence")) {
    return ["Document", "Intel"];
  }
  if (normalized.includes("lineage")) {
    return ["Lineage"];
  }
  if (normalized.includes("event ledger")) {
    return ["Event", "Ledger"];
  }
  if (normalized.includes("trace")) {
    return ["Trace"];
  }
  const words = text.split(/\s+/).filter(Boolean);
  if (words.length <= 2) {
    return words;
  }
  return [words[0], words[words.length - 1]];
}

function chartPoints(items, key, chartWidth, chartHeight, maxValue) {
  if (!items.length || maxValue <= 0) {
    return [];
  }
  return items.map((item, index) => ({
    x: anchorX(index, items.length),
    y: CHART_DIMENSIONS.marginTop + chartHeight - ((item[key] || 0) / maxValue) * chartHeight,
    value: item[key] || 0,
    label: item.label,
  }));
}

function smoothLinePath(points) {
  if (!points.length) {
    return "";
  }
  if (points.length === 1) {
    return `M ${points[0].x} ${points[0].y}`;
  }
  let path = `M ${points[0].x} ${points[0].y}`;
  for (let index = 1; index < points.length - 1; index += 1) {
    const current = points[index];
    const next = points[index + 1];
    const midX = (current.x + next.x) / 2;
    const midY = (current.y + next.y) / 2;
    path += ` Q ${current.x} ${current.y} ${midX} ${midY}`;
  }
  const last = points[points.length - 1];
  path += ` T ${last.x} ${last.y}`;
  return path;
}

function areaPath(points, height) {
  if (!points.length) {
    return "";
  }
  const baseline = CHART_DIMENSIONS.marginTop + height;
  return `${smoothLinePath(points)} L ${points[points.length - 1].x} ${baseline} L ${points[0].x} ${baseline} Z`;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function tooltipLeftForIndex(index, itemsLength) {
  if (!itemsLength) {
    return CHART_DIMENSIONS.tooltipInset;
  }
  const pointX = anchorX(index, itemsLength);
  return clamp(
    pointX - CHART_DIMENSIONS.tooltipWidth / 2,
    CHART_DIMENSIONS.tooltipInset,
    CHART_DIMENSIONS.width - CHART_DIMENSIONS.tooltipWidth - CHART_DIMENSIONS.tooltipInset,
  );
}

function TrendChart({ items, series, valueKeys, scoreMode = false, title }) {
  const width = CHART_DIMENSIONS.width;
  const height = CHART_DIMENSIONS.height;
  const totalHeight = height + CHART_DIMENSIONS.marginTop + CHART_DIMENSIONS.marginBottom;
  const [hoveredIndex, setHoveredIndex] = useState(null);
  const trendMax = Math.max(
    1,
    ...(scoreMode ? [100] : items.flatMap((item) => valueKeys.map((key) => item[key]))),
  );
  const seriesPoints = useMemo(
    () => Object.fromEntries(series.map((seriesItem) => [seriesItem.key, chartPoints(items, seriesItem.key, width, height, trendMax)])),
    [height, items, series, trendMax, width],
  );
  const activeIndex = hoveredIndex == null ? items.length - 1 : hoveredIndex;
  const activeItem = items[activeIndex] || null;

  return (
    <div className="trend-chart-shell">
      <div className="artifact-column-head">
        <strong>{title}</strong>
      </div>
      <div className="trend-chart-frame">
        <svg className="trend-chart" viewBox={`0 0 ${width} ${totalHeight}`} role="img" aria-label={title}>
          <defs>
            {series.map((seriesItem) => (
              <linearGradient id={`trend-fill-${seriesItem.key}`} key={seriesItem.key} x1="0" x2="0" y1="0" y2="1">
                <stop offset="0%" stopColor={seriesItem.color} stopOpacity="0.26" />
                <stop offset="100%" stopColor={seriesItem.color} stopOpacity="0.02" />
              </linearGradient>
            ))}
          </defs>

          {[0.25, 0.5, 0.75, 1].map((ratio) => (
            <line
              key={ratio}
              className="trend-grid-line"
              x1={CHART_DIMENSIONS.marginLeft}
              x2={width - CHART_DIMENSIONS.marginRight}
              y1={CHART_DIMENSIONS.marginTop + height - height * ratio}
              y2={CHART_DIMENSIONS.marginTop + height - height * ratio}
            />
          ))}

          {series.map((seriesItem) => (
            <g key={seriesItem.key}>
              <path d={areaPath(seriesPoints[seriesItem.key], height)} className="trend-area" fill={`url(#trend-fill-${seriesItem.key})`} />
              <path d={smoothLinePath(seriesPoints[seriesItem.key])} className="trend-line" stroke={seriesItem.color} />
              {seriesPoints[seriesItem.key].map((point, index) => (
                <circle
                  key={`${seriesItem.key}-${point.label}`}
                  className={`trend-point ${index === activeIndex ? "trend-point--active" : ""}`}
                  cx={point.x}
                  cy={point.y}
                  r={index === activeIndex ? 5.5 : 3.5}
                  fill={seriesItem.color}
                />
              ))}
            </g>
          ))}

          {items.map((item, index) => {
            const x = anchorX(index, items.length);
            const stepWidth = plotWidth() / Math.max(items.length, 1);
            const compactLines = compactAxisLabelLines(item.label);
            const labelY = totalHeight - (compactLines.length > 1 ? 24 : 14);
            return (
              <g key={item.label}>
                <rect
                  x={Math.max(CHART_DIMENSIONS.marginLeft, x - stepWidth / 2)}
                  y={CHART_DIMENSIONS.marginTop}
                  width={stepWidth}
                  height={height}
                  className="trend-hover-zone"
                  onMouseEnter={() => setHoveredIndex(index)}
                  onMouseLeave={() => setHoveredIndex(null)}
                />
                <text className="trend-axis-label trend-axis-label--compact" x={x} y={labelY} textAnchor="middle">
                  {compactLines.map((line, lineIndex) => (
                    <tspan key={`${item.label}-${line}`} x={x} dy={lineIndex === 0 ? 0 : 11}>
                      {line}
                    </tspan>
                  ))}
                </text>
              </g>
            );
          })}
        </svg>

        {activeItem ? (
            <div className="trend-tooltip" style={{ left: `${tooltipLeftForIndex(activeIndex || 0, items.length)}px` }}>
            <strong>{activeItem.label}</strong>
            {series.map((seriesItem) => (
              <span key={seriesItem.key}>
                <i style={{ background: seriesItem.color }} />
                {seriesItem.label}: {formatCompactNumber(activeItem[seriesItem.key])}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="trend-legend">
        {series.map((seriesItem) => (
          <span className="legend-pill" key={seriesItem.key}>
            <i style={{ background: seriesItem.color }} />
            {seriesItem.label}
          </span>
        ))}
      </div>
    </div>
  );
}

function TrendPanel({ items = [] }) {
  const scoreSeries = [
    { key: "healthScore", label: "Health score", color: "#60a5fa" },
    { key: "driftScore", label: "Drift risk", color: "#c084fc" },
  ];

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">Validation Trends</p>
          <h2>Latest end-to-end validation signal</h2>
        </div>
      </div>

      <div className="dashboard-two-column dashboard-two-column--dense">
        <TrendChart
          items={items}
          series={SERIES}
          valueKeys={["passed", "failed", "warned"]}
          title="Checks by contract stage"
        />
        <TrendChart
          items={items}
          series={scoreSeries}
          valueKeys={["healthScore", "driftScore"]}
          scoreMode
          title="Health and drift risk"
        />
      </div>

      <div className="mini-grid">
        {items.map((item) => (
          <article className="mini-stat-card" key={item.label}>
            <div className="mini-stat-top">
              <strong>{item.label}</strong>
              <span className={`badge badge--${getStatusTone(item.status)}`}>{item.status}</span>
            </div>
            <p>
              {formatCompactNumber(item.passed)} passed • {formatCompactNumber(item.failed)} failed • {formatCompactNumber(item.warned)} warned
            </p>
            <p className="muted-copy">
              Health {item.healthScore}/100 • Drift risk {item.driftScore}/100
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}

export default TrendPanel;
