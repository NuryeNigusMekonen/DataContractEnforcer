export function formatTimestamp(value) {
  if (!value) {
    return "Unavailable";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(parsed);
}

export function formatTimeValue(value) {
  if (!value) {
    return "--";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(parsed);
}

export function formatCompactNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  return new Intl.NumberFormat(undefined, {
    notation: value > 999 ? "compact" : "standard",
    maximumFractionDigits: 1,
  }).format(Number(value));
}

export function formatPercent(value, maximumFractionDigits = 1) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return String(value);
  }
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    maximumFractionDigits,
  }).format(numeric);
}

function stringifyChangeValue(value) {
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  if (value === null || value === undefined || value === "") {
    return "n/a";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function formatChangeSummary(change) {
  if (!change) {
    return "No simulation loaded.";
  }
  return `${change.field}: ${stringifyChangeValue(change.from)} -> ${stringifyChangeValue(change.to)}`;
}
