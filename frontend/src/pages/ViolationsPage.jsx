import { useEffect, useMemo, useState } from "react";

import ViolationWorkbench from "../components/ViolationWorkbench";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchViolationsPageData } from "../services/api";

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

function ViolationsPage({ refreshToken, navigate, locationSearch }) {
  const { data, loading, error } = useCachedPageData(fetchViolationsPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 12000,
  });
  const [selectedViolation, setSelectedViolation] = useState(null);
  const selectedFromLocation = useMemo(() => new URLSearchParams(locationSearch || "").get("violation") || "", [locationSearch]);

  useEffect(() => {
    const violations = data?.violations || [];
    const storedViolationId = window.localStorage.getItem("dashboard-selected-violation") || "";
    const storedViolationKey = window.localStorage.getItem("dashboard-selected-violation-key") || "";
    const selectedKey = violationSelectionKey(selectedViolation);

    if (!violations.length) {
      setSelectedViolation(null);
      return;
    }

    if (selectedFromLocation) {
      const matched = violations.find((item) => item.violation_id === selectedFromLocation);
      if (matched) {
        setSelectedViolation(matched);
        return;
      }
    }

    if (selectedViolation) {
      const matchedById = violations.find((item) => item.violation_id === selectedViolation.violation_id);
      if (matchedById) {
        if (matchedById !== selectedViolation) {
          setSelectedViolation(matchedById);
        }
        return;
      }

      if (selectedKey) {
        const matchedByKey = violations.find((item) => violationSelectionKey(item) === selectedKey);
        if (matchedByKey) {
          setSelectedViolation(matchedByKey);
          window.localStorage.setItem("dashboard-selected-violation", matchedByKey.violation_id || "");
          window.localStorage.setItem("dashboard-selected-violation-key", violationSelectionKey(matchedByKey));
          return;
        }
      }
    }

    if (storedViolationId) {
      const matched = violations.find((item) => item.violation_id === storedViolationId);
      if (matched) {
        setSelectedViolation(matched);
        return;
      }
    }

    if (storedViolationKey) {
      const matched = violations.find((item) => violationSelectionKey(item) === storedViolationKey);
      if (matched) {
        setSelectedViolation(matched);
        window.localStorage.setItem("dashboard-selected-violation", matched.violation_id || "");
        return;
      }
    }
    setSelectedViolation(violations[0] || null);
  }, [data?.violations, selectedFromLocation, selectedViolation]);

  function handleSelect(violation) {
    setSelectedViolation(violation);
    window.localStorage.setItem("dashboard-selected-violation", violation.violation_id);
    window.localStorage.setItem("dashboard-selected-violation-key", violationSelectionKey(violation));
    navigate(`/violations?violation=${encodeURIComponent(violation.violation_id)}`, { replace: true });
  }

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading violations…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}
      <ViolationWorkbench
        violations={data?.violations || []}
        selectedViolation={selectedViolation}
        onSelect={handleSelect}
        onNavigate={navigate}
      />
    </div>
  );
}

export default ViolationsPage;
