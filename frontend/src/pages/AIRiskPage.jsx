import AIRiskPanel from "../components/AIRiskPanel";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchAiRiskPageData } from "../services/api";
import { aiRiskStatus } from "../utils/dashboardTransforms";
import { formatPercent, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function AIRiskPage({ refreshToken }) {
  const { data, loading, error } = useCachedPageData(fetchAiRiskPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 15000,
  });

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading AI risk view…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  const aiRisk = data?.enforcerReport?.ai_system_risk_assessment || {};
  const overallStatus = aiRiskStatus(aiRisk);
  const structuredOutput = aiRisk?.structured_llm_output_enforcement || aiRisk?.llm_output_schema_rate || {};
  const traceContracts = aiRisk?.langsmith_trace_schema_contracts || {};

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}

      <section className="panel">
        <div className="panel-heading">
          <div>
            <p className="section-kicker">AI Risk Summary</p>
            <h2>Current model-output safeguards</h2>
          </div>
          <span className={`badge badge--${getStatusTone(overallStatus)}`}>{overallStatus}</span>
        </div>

        <div className="detail-split">
          <article className="list-card">
            <strong>Monitoring policy</strong>
            <p>
              Structured output warns at {formatPercent(structuredOutput.warn_threshold || 0)} and fails at {formatPercent(structuredOutput.fail_threshold || 0)}; current invalid-output rate is {formatPercent(structuredOutput.violation_rate || 0)}.
            </p>
          </article>
          <article className="list-card">
            <strong>Trace contract posture</strong>
            <p>
              {traceContracts.failed_contract_checks || 0} failing LangSmith checks across {traceContracts.total_records || 0} trace rows. Last refresh {formatTimestamp(data?.enforcerReport?.generated_at)}.
            </p>
          </article>
        </div>
      </section>

      <AIRiskPanel aiRisk={aiRisk} />
    </div>
  );
}

export default AIRiskPage;
