import IncidentPanel from "../components/IncidentPanel";
import MetricOverview, { buildOverviewItems } from "../components/MetricOverview";
import WhatIfPanel from "../components/WhatIfPanel";
import WeekStatusGrid from "../components/WeekStatusGrid";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchOverviewPageData } from "../services/api";
import {
  aiRiskStatus,
  fallbackIncidentFromTimeline,
} from "../utils/dashboardTransforms";
import { getStatusTone } from "../utils/status";
import { replaceSystemNames } from "../utils/systemNames";

const QUICK_LINKS = [
  {
    title: "Validation detail",
    description: "Inspect trends, contract stage results, and run-by-run status.",
    path: "/validation",
  },
  {
    title: "Violations evidence",
    description: "Review failing checks, samples, and expected-versus-actual output.",
    path: "/violations",
  },
  {
    title: "Attribution",
    description: "Trace likely source commits, blast radius, and downstream exposure.",
    path: "/attribution",
  },
  {
    title: "Artifacts",
    description: "Browse the published contracts, reports, snapshots, and logs.",
    path: "/artifacts",
  },
];

function OverviewPage({ refreshToken, navigate }) {
  const { data, loading, error } = useCachedPageData(fetchOverviewPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 12000,
  });

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading overview…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  const aiRisk = data?.enforcerReport?.ai_system_risk_assessment || {};
  const overviewItems = buildOverviewItems({
    kpi: data?.kpi || {},
    summary: data?.summary || {},
    violations: data?.violations || [],
    aiRiskStatus: aiRiskStatus(aiRisk),
  });
  const topRisks = (data?.violations || []).slice(0, 3);

  return (
    <div className="page-stack overview-page overview-page--compact">
      {error ? <div className="error-banner">{error}</div> : null}
      <MetricOverview items={overviewItems} />

      <section className="dashboard-two-column">
        <IncidentPanel
          incident={data?.incident || {}}
          fallbackIncident={fallbackIncidentFromTimeline(data?.timeline || { items: [] })}
        />
        <section className="panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">Key Risks</p>
              <h2>Where to investigate next</h2>
            </div>
          </div>

          <div className="stack-list">
            {topRisks.length ? topRisks.map((violation) => (
              <article className="list-card" key={violation.violation_id}>
                <div className="list-card-top">
                  <strong>{violation.field || violation.check_id || "Contract issue"}</strong>
                  <span className={`badge badge--${getStatusTone(violation.severity)}`}>{violation.severity || "UNKNOWN"}</span>
                </div>
                <p>{replaceSystemNames(violation.short_message || violation.message)}</p>
                <div className="button-row button-row--compact">
                  <button className="text-button" type="button" onClick={() => navigate(`/violations?violation=${encodeURIComponent(violation.violation_id)}`)}>
                    Open violation detail
                  </button>
                  <button className="text-button" type="button" onClick={() => navigate(`/attribution?violation=${encodeURIComponent(violation.violation_id)}`)}>
                    Open attribution
                  </button>
                </div>
              </article>
            )) : (
              <div className="empty-state empty-state--compact">
                <strong>No active risks</strong>
                <p className="muted-copy">The current published run has no open platform validation issues.</p>
              </div>
            )}
          </div>
        </section>
      </section>

      <section className="dashboard-two-column dashboard-two-column--dense">
        <WeekStatusGrid weeks={data?.weeks || []} updatedWeekKeys={[]} />

        <div className="overview-secondary-stack">
          <section className="panel">
            <div className="panel-heading">
              <div>
                <p className="section-kicker">Quick Links</p>
                <h2>Go straight to the right workspace</h2>
              </div>
            </div>

            <p className="muted-copy">{data?.summary?.health_narrative || "Choose a deeper page based on the question you need to answer."}</p>

            <div className="overview-link-grid">
              {QUICK_LINKS.map((link) => (
                <button key={link.path} className="overview-link-card" type="button" onClick={() => navigate(link.path)}>
                  <strong>{link.title}</strong>
                  <p>{link.description}</p>
                </button>
              ))}
            </div>
          </section>

          <WhatIfPanel whatIf={data?.whatIf || { available_specs: [] }} />
        </div>
      </section>
    </div>
  );
}

export default OverviewPage;
