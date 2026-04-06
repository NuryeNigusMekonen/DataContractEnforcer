const PLATFORM_FULL_NAME = "Contract Enforcer";
const PLATFORM_SHORT_NAME = "Contract Enforcer";

const SYSTEM_NAME_SPECS = [
  {
    key: "week1",
    week: "Week 1",
    full: "Intent Engine",
    short: "Intent Engine",
    markers: ["week 1", "week-1", "week_1", "week1", "week1-intent-tracker", "week1-intent-records"],
  },
  {
    key: "week2",
    week: "Week 2",
    full: "Governance Engine",
    short: "Governance Engine",
    markers: ["week 2", "week-2", "week_2", "week2", "week2-digital-courtroom", "week2-verdict-records"],
  },
  {
    key: "week3",
    week: "Week 3",
    full: "Document Intelligence",
    short: "Document Intelligence",
    markers: ["week 3", "week-3", "week_3", "week3", "week3-document-refinery", "week3-document-refinery-extractions"],
  },
  {
    key: "week4",
    week: "Week 4",
    full: "Lineage Mapper",
    short: "Lineage Mapper",
    markers: ["week 4", "week-4", "week_4", "week4", "week4-brownfield-cartographer", "week4-lineage-snapshots"],
  },
  {
    key: "week5",
    week: "Week 5",
    full: "Event Ledger",
    short: "Event Ledger",
    markers: ["week 5", "week-5", "week_5", "week5", "week5-ledger", "week5-event-records"],
  },
  {
    key: "week7",
    week: "Week 7",
    full: PLATFORM_FULL_NAME,
    short: PLATFORM_SHORT_NAME,
    markers: ["week 7", "week-7", "week_7", "week7", "data contract enforcer", "week7-validation-runner", "week7-violation-attributor"],
  },
  {
    key: "traces",
    week: "Traces",
    full: "Trace Monitor",
    short: "Trace Monitor",
    markers: ["traces", "langsmith-trace-pipeline", "langsmith-trace-records", "trace-records"],
  },
];

function normalizeValue(value) {
  return value == null ? "" : String(value);
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function replacementForSpec(spec, { short = false, style = "system" } = {}) {
  if (style === "week") {
    return spec.week;
  }
  return short ? spec.short : spec.full;
}

function markerEntries({ short = false, style = "system" } = {}) {
  return SYSTEM_NAME_SPECS
    .flatMap((spec) => {
      const allMarkers = [...new Set([...spec.markers, spec.full, spec.short, spec.week])];
      return allMarkers.map((marker) => ({ marker, replacement: replacementForSpec(spec, { short, style }) }));
    })
    .sort((left, right) => right.marker.length - left.marker.length);
}

function findSystemSpec(value) {
  const candidate = normalizeValue(value).toLowerCase();
  return SYSTEM_NAME_SPECS.find((spec) => spec.markers.some((marker) => candidate.includes(marker)));
}

function getSystemDisplayName(value, { short = false, fallback } = {}) {
  const spec = findSystemSpec(value);
  if (spec) {
    return short ? spec.short : spec.full;
  }
  const normalizedFallback = normalizeValue(fallback);
  if (normalizedFallback) {
    return normalizedFallback;
  }
  return normalizeValue(value);
}

function replaceSystemNames(value, { short = false } = {}) {
  let text = normalizeValue(value);
  if (!text) {
    return text;
  }

  markerEntries({ short, style: "system" }).forEach(({ marker, replacement }) => {
    text = text.replace(new RegExp(escapeRegExp(marker), "gi"), replacement);
  });

  return text;
}

function replaceWeekNames(value) {
  let text = normalizeValue(value);
  if (!text) {
    return text;
  }

  markerEntries({ style: "week" }).forEach(({ marker, replacement }) => {
    text = text.replace(new RegExp(escapeRegExp(marker), "gi"), replacement);
  });

  return text;
}

function formatSystemList(values, { short = false, empty = "No downstream systems listed" } = {}) {
  if (!values?.length) {
    return empty;
  }
  return values.map((item) => replaceSystemNames(item, { short })).join(", ");
}

export {
  PLATFORM_FULL_NAME,
  PLATFORM_SHORT_NAME,
  getSystemDisplayName,
  replaceSystemNames,
  replaceWeekNames,
  formatSystemList,
};