import { PLATFORM_FULL_NAME } from "../utils/systemNames";

function CompactPageHeader({
  route,
  navigate,
  supplementalAction,
  className = "",
}) {
  const linkedViewCount = route.actions?.length || 0;
  const hasActions = linkedViewCount || supplementalAction;

  return (
    <header className={`page-header ${className}`.trim()}>
      <div className={`compact-page-header compact-page-header--minimal compact-page-header--polished ${hasActions ? "compact-page-header--with-actions" : ""}`.trim()}>
        <div className="compact-page-header__copy">
          <p className="section-kicker">{PLATFORM_FULL_NAME} / {route.label}</p>
          <h1>{route.title}</h1>
          {route.description ? <p className="header-copy">{route.description}</p> : null}
          <div className="compact-page-header__chips" aria-label="Workspace summary">
            <span className="badge badge--neutral">Client-ready workspace</span>
            {linkedViewCount ? <span className="badge badge--neutral">{linkedViewCount} linked view{linkedViewCount === 1 ? "" : "s"}</span> : null}
          </div>
        </div>

        {hasActions ? (
          <div className="page-header__action-panel">
            <span className="micro-label">Workspace controls</span>
            <div className="page-header__actions" aria-label="Page actions">
              {supplementalAction ? (
                <button
                  className="primary-button primary-button--ghost"
                  type="button"
                  onClick={supplementalAction.onClick}
                >
                  {supplementalAction.label}
                </button>
              ) : null}
              {route.actions.map((action) => (
                <button
                  key={`${route.id}-${action.path}`}
                  className="text-button"
                  type="button"
                  onClick={() => navigate(action.path)}
                >
                  {action.label}
                </button>
              ))}
            </div>
            <p className="page-header__action-meta">Use the linked views to move directly into evidence, schema, attribution, or artifact review.</p>
          </div>
        ) : null}
      </div>
    </header>
  );
}

export default CompactPageHeader;
