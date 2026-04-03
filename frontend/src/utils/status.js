const STATUS_TONES = {
  PASS: "pass",
  COMPATIBLE: "pass",
  OK: "pass",
  HEALTHY: "pass",
  WARN: "warn",
  MEDIUM: "warn",
  UNKNOWN: "neutral",
  FAIL: "fail",
  ERROR: "fail",
  HIGH: "fail",
  CRITICAL: "fail",
  BREAKING: "breaking",
  FORWARD_COMPATIBLE: "breaking",
  BREAKING_CHANGE: "breaking",
  NOT_ATTEMPTED: "neutral",
};

export function getStatusTone(value) {
  return STATUS_TONES[String(value || "UNKNOWN").toUpperCase()] || "neutral";
}
