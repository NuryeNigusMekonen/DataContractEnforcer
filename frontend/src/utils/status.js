const STATUS_TONES = {
  PASS: "pass",
  COMPATIBLE: "pass",
  BACKWARD_COMPATIBLE: "pass",
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
  BREAKING_BUT_ADAPTABLE: "warn",
  BREAKING_REQUIRES_MIGRATION: "breaking",
  FORWARD_COMPATIBLE: "breaking",
  BREAKING_CHANGE: "breaking",
  REQUIRED: "breaking",
  NO_ADAPTER_NEEDED: "pass",
  NOT_ATTEMPTED: "neutral",
};

export function getStatusTone(value) {
  return STATUS_TONES[String(value || "UNKNOWN").toUpperCase()] || "neutral";
}
