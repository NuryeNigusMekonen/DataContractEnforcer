function DemoModeBar({
  scenarios = [],
  specs = [],
  selectedScenario,
  onScenarioChange,
  onRestoreHealthy,
  onRunFailureDemo,
  selectedSpec,
  onSpecChange,
  onRunWhatIf,
  busyLabel,
}) {
  return (
    <section className="panel demo-bar">
      <div className="panel-heading panel-heading--tight">
        <div>
          <p className="section-kicker">Simulation Controls</p>
          <h2>Run healthy, failure, and what-if scenarios without changing the backend flow</h2>
        </div>
        {busyLabel ? <span className="badge badge--warn">{busyLabel}</span> : null}
      </div>

      <div className="demo-grid">
        <div className="demo-actions">
          <label className="field">
            <span>Failure scenario</span>
            <select value={selectedScenario} onChange={(event) => onScenarioChange(event.target.value)}>
              {scenarios.map((scenario) => (
                <option key={scenario.id} value={scenario.path}>
                  {scenario.label}
                </option>
              ))}
            </select>
          </label>
          <div className="button-row">
            <button className="primary-button" type="button" onClick={onRestoreHealthy} disabled={!scenarios.length}>
              Restore healthy baseline
            </button>
            <button className="primary-button primary-button--danger" type="button" onClick={onRunFailureDemo} disabled={!selectedScenario}>
              Run failure demo
            </button>
          </div>
        </div>

        <div className="demo-actions">
          <label className="field">
            <span>What-if simulation</span>
            <select value={selectedSpec} onChange={(event) => onSpecChange(event.target.value)}>
              {specs.map((spec) => (
                <option key={spec.id} value={spec.path || spec.id}>
                  {spec.label}
                </option>
              ))}
            </select>
          </label>
          <div className="button-row">
            <button className="primary-button primary-button--ghost" type="button" onClick={onRunWhatIf} disabled={!selectedSpec}>
              Run what-if
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}

export default DemoModeBar;
