import { getSystemDisplayName, replaceSystemNames } from "./systemNames";

function aiRiskStatus(aiRisk) {
  const structuredOutput = aiRisk?.structured_llm_output_enforcement || aiRisk?.llm_output_schema_rate;
  const statuses = [
    aiRisk?.embedding_drift?.status,
    aiRisk?.prompt_input_validation?.status,
    structuredOutput?.status,
    aiRisk?.langsmith_trace_schema_contracts?.status,
  ].map((value) => String(value || "UNKNOWN").toUpperCase());
  if (statuses.some((value) => ["FAIL", "ERROR", "CRITICAL", "HIGH"].includes(value))) {
    return "FAIL";
  }
  if (statuses.some((value) => value === "WARN")) {
    return "WARN";
  }
  if (statuses.some((value) => value === "PASS")) {
    return "PASS";
  }
  return "UNKNOWN";
}

function healthScore(passed, warned, total) {
  if (!total) {
    return 100;
  }
  return Math.max(0, Math.min(100, Math.round(((passed + warned * 0.5) / total) * 100)));
}

function inferDriftScore(item) {
  const base = (item.change_count || 0) * 15;
  const warnScore = Number(item.warned || item.checks_warned || 0) * 2;
  const failScore = Number(item.failed || item.checks_failed || 0) * 4;
  return Math.min(100, base + warnScore + failScore);
}

function buildTrendItems(artifacts, weeks) {
  const reportMap = new Map((artifacts?.validation_reports || []).map((item) => [item.contract_id, item]));
  return (weeks || []).map((week) => {
    const report = reportMap.get(week.contract_id);
    const passed = Number(report?.passed ?? week.checks_passed ?? 0);
    const warned = Number(report?.warned ?? week.checks_warned ?? 0);
    const failed = Number(report?.failed ?? week.checks_failed ?? 0);
    const total = Number(report?.total_checks ?? week.total_checks ?? 0);
    return {
      label: getSystemDisplayName(week.week_name || week.contract_id, { short: true, fallback: replaceSystemNames(week.week_name || week.contract_id, { short: true }) }),
      status: week.status,
      passed,
      warned,
      failed,
      healthScore: report?.health_score ?? healthScore(passed, warned, total),
      driftScore: report?.drift_score ?? inferDriftScore(week),
    };
  });
}

function inferUpstreamSystem(path, contractId) {
  const candidate = String(path || contractId || "").toLowerCase();
  if (candidate.includes("week1")) {
    return getSystemDisplayName("week1", { fallback: "Intent Engine" });
  }
  if (candidate.includes("week2")) {
    return getSystemDisplayName("week2", { fallback: "Governance Engine" });
  }
  if (candidate.includes("week3")) {
    return getSystemDisplayName("week3", { fallback: "Document Intelligence" });
  }
  if (candidate.includes("week4")) {
    return getSystemDisplayName("week4", { fallback: "Lineage Mapper" });
  }
  if (candidate.includes("week5")) {
    return getSystemDisplayName("week5", { fallback: "Event Ledger" });
  }
  if (candidate.includes("trace")) {
    return getSystemDisplayName("traces", { fallback: "Trace Monitor" });
  }
  return "unknown-producer";
}

function buildCoverageRows(contracts, weeks, schemaEvolution) {
  const weekMap = new Map((weeks || []).map((item) => [item.contract_id, item]));
  return (contracts || []).map((contract) => {
    const week = weekMap.get(contract.contract_id) || {};
    return {
      contractId: contract.contract_id,
      contractName: getSystemDisplayName(contract.title || contract.contract_id, {
        fallback: replaceSystemNames(contract.title || contract.contract_id),
      }),
      dataset: contract.dataset,
      upstreamSystem: inferUpstreamSystem(contract.source_path, contract.contract_id),
      downstreamSystem: contract.downstream_labels?.length
        ? contract.downstream_labels.map((label) => replaceSystemNames(label)).join(", ")
        : "No downstream consumers",
      validationActive: Boolean(week.total_checks),
      attributionActive: contract.downstream_count > 0,
      schemaEvolutionTracking: Boolean(schemaEvolution?.contract_id) || Boolean(week.contract_id),
      aiExtensionsApplied: Boolean(contract.ai_extensions_applied),
      status: week.status || "UNKNOWN",
    };
  });
}

function fallbackIncidentFromTimeline(timeline) {
  const lastViolation = (timeline?.items || []).find((item) => item.category === "violation");
  if (!lastViolation) {
    return {
      severity: "PASS",
      title: "No recent resolved incident",
      message: "Recent runs are healthy and no historical violation event is available for fallback display.",
      detected_at: null,
      field: null,
      affected_systems: [],
    };
  }
  return {
    severity: lastViolation.status,
    title: replaceSystemNames(lastViolation.title),
    message: replaceSystemNames(lastViolation.details),
    detected_at: lastViolation.timestamp || lastViolation.time,
    field: replaceSystemNames(lastViolation.source),
    week: replaceSystemNames(lastViolation.title),
    affected_systems: [],
  };
}

function computeFocusNodeIds(selectedViolation, lineageMap) {
  if (!selectedViolation) {
    return [];
  }
  const weekToken = String(selectedViolation.week || "")
    .toLowerCase()
    .replace(/\s+/g, "");
  const affectedSystems = (selectedViolation.affected_systems || []).map((item) => String(item).toLowerCase());

  return (lineageMap?.cross_week?.nodes || [])
    .filter((node) => {
      const haystack = `${node.id} ${node.label} ${node.path}`.toLowerCase();
      if (weekToken && haystack.includes(weekToken)) {
        return true;
      }
      if (affectedSystems.some((system) => haystack.includes(system))) {
        return true;
      }
      return haystack.includes("week7-validation-runner") || haystack.includes("week7-violation-attributor");
    })
    .map((node) => node.id);
}

function preferredScenarioPath(scenarios, fragment) {
  return scenarios.find((scenario) => scenario.path.includes(fragment))?.path || scenarios[0]?.path || "";
}

export {
  aiRiskStatus,
  buildCoverageRows,
  buildTrendItems,
  computeFocusNodeIds,
  fallbackIncidentFromTimeline,
  preferredScenarioPath,
};
