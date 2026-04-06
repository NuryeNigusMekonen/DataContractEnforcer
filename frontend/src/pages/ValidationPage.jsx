import TrendPanel from "../components/TrendPanel";
import WeekStatusGrid from "../components/WeekStatusGrid";
import useCachedPageData from "../hooks/useCachedPageData";
import { fetchValidationPageData } from "../services/api";
import { buildTrendItems } from "../utils/dashboardTransforms";
import { getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function ValidationPage({ refreshToken, navigate }) {
  const { data, loading, error } = useCachedPageData(fetchValidationPageData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 15000,
  });

  if (loading && !data) {
    return <div className="empty-state"><strong>Loading validation view…</strong></div>;
  }

  if (error && !data) {
    return <div className="error-banner">{error}</div>;
  }

  const trendItems = buildTrendItems(data?.artifacts || {}, data?.weeks || []);

  return (
    <div className="page-stack">
      {error ? <div className="error-banner">{error}</div> : null}
      <TrendPanel items={trendItems} />

      <section className="dashboard-two-column">
        <WeekStatusGrid weeks={data?.weeks || []} updatedWeekKeys={[]} onNavigate={navigate} />

        <section className="panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">Validation Runs</p>
              <h2>System validation breakdown</h2>
            </div>
            <div className="page-inline-actions">
              <button className="text-button" type="button" onClick={() => navigate("/schema-evolution")}>
                Schema summary
              </button>
              <button className="text-button" type="button" onClick={() => navigate("/violations")}>
                Open violations
              </button>
            </div>
          </div>

          <div className="table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Week</th>
                  <th>Status</th>
                  <th>Passed</th>
                  <th>Failed</th>
                  <th>Warned</th>
                  <th>Total</th>
                  <th>Updated</th>
                  <th>Next step</th>
                </tr>
              </thead>
              <tbody>
                {(data?.validations || []).map((validation) => (
                  <tr key={validation.contract_id}>
                    <td>{getSystemDisplayName(validation.week_name || validation.contract_id, { short: true, fallback: replaceSystemNames(validation.week_name || validation.contract_id, { short: true }) })}</td>
                    <td>{validation.status}</td>
                    <td>{validation.passed}</td>
                    <td>{validation.failed}</td>
                    <td>{validation.warned}</td>
                    <td>{validation.total_checks}</td>
                    <td>{validation.last_updated || "Unavailable"}</td>
                    <td>
                      <div className="table-action-row">
                        {(validation.failed || validation.status === "FAIL" || validation.status === "ERROR") ? (
                          <button className="text-button" type="button" onClick={() => navigate("/violations")}>
                            View violations
                          </button>
                        ) : null}
                        {(validation.warned || validation.status === "WARN") ? (
                          <button className="text-button" type="button" onClick={() => navigate("/schema-evolution")}>
                            Check schema
                          </button>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </section>
    </div>
  );
}

export default ValidationPage;
