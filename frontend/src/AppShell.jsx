import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";

import CommandCenter from "./components/CommandCenter";
import CompactPageHeader from "./components/CompactPageHeader";
import GlobalUtilityBar from "./components/GlobalUtilityBar";
import useCachedPageData from "./hooks/useCachedPageData";
import {
  fetchJob,
  fetchShellData,
  invalidateDashboardCache,
  publishMode,
  regenerateScenario,
  runWhatIf,
} from "./services/api";
import { preferredScenarioPath } from "./utils/dashboardTransforms";
import { PLATFORM_FULL_NAME, PLATFORM_SHORT_NAME } from "./utils/systemNames";

const OverviewPage = lazy(() => import("./pages/OverviewPage"));
const ValidationPage = lazy(() => import("./pages/ValidationPage"));
const ViolationsPage = lazy(() => import("./pages/ViolationsPage"));
const AttributionPage = lazy(() => import("./pages/AttributionPage"));
const SchemaEvolutionPage = lazy(() => import("./pages/SchemaEvolutionPage"));
const AIRiskPage = lazy(() => import("./pages/AIRiskPage"));
const LineagePage = lazy(() => import("./pages/LineagePage"));
const ArtifactsPage = lazy(() => import("./pages/ArtifactsPage"));

const ROUTES = [
  {
    id: "overview",
    path: "/",
    label: "Overview",
    shortLabel: "OV",
    title: "Overview",
    description: "Critical KPIs, current incident state, and a quick validation summary.",
    component: OverviewPage,
  },
  {
    id: "validation",
    path: "/validation",
    label: "Validation",
    shortLabel: "VA",
    title: "Validation",
    description: "Trend analysis, health and drift-risk scoring, and system validation detail.",
    actions: [
      { label: "Violations", path: "/violations" },
      { label: "Schema", path: "/schema-evolution" },
    ],
    component: ValidationPage,
  },
  {
    id: "violations",
    path: "/violations",
    label: "Violations",
    shortLabel: "VI",
    title: "Violations",
    description: "Searchable violations with selected-check evidence and failing samples.",
    actions: [
      { label: "Attribution", path: "/attribution" },
    ],
    supportsFocusMode: true,
    component: ViolationsPage,
  },
  {
    id: "attribution",
    path: "/attribution",
    label: "Attribution",
    shortLabel: "AT",
    title: "Attribution",
    description: "Blame chains, blast radius, and traversal context for active issues.",
    actions: [
      { label: "Lineage", path: "/lineage" },
    ],
    component: AttributionPage,
  },
  {
    id: "schema",
    path: "/schema-evolution",
    label: "Schema Evolution",
    shortLabel: "SC",
    title: "Schema Evolution",
    description: "Compatibility changes, migration impact, and rollback guidance.",
    actions: [
      { label: "Artifacts", path: "/artifacts" },
    ],
    component: SchemaEvolutionPage,
  },
  {
    id: "ai-risk",
    path: "/ai-risk",
    label: "AI Risk",
    shortLabel: "AI",
    title: "AI Risk",
    description: "Embedding drift, prompt validation, and LLM schema outputs in one place.",
    actions: [
      { label: "Validation", path: "/validation" },
    ],
    component: AIRiskPage,
  },
  {
    id: "lineage",
    path: "/lineage",
    label: "Lineage",
    shortLabel: "LG",
    title: "Lineage",
    description: "Explore the full graph with search, filtering, and issue-path highlighting.",
    actions: [
      { label: "Artifacts", path: "/artifacts" },
    ],
    component: LineagePage,
  },
  {
    id: "artifacts",
    path: "/artifacts",
    label: "Artifacts",
    shortLabel: "AR",
    title: "Artifacts",
    description: "Review current contracts, reports, snapshots, logs, and file previews.",
    actions: [
      { label: "Validation", path: "/validation" },
    ],
    supportsFocusMode: true,
    component: ArtifactsPage,
  },
];

function loadStoredMap(key) {
  try {
    return JSON.parse(window.localStorage.getItem(key) || "{}") || {};
  } catch {
    return {};
  }
}

function locationKey(state) {
  return `${state.pathname}${state.search}`;
}

function normalizePath(pathname) {
  return pathname.replace(/\/+$/, "") || "/";
}

function currentLocationState() {
  return {
    pathname: normalizePath(window.location.pathname),
    search: window.location.search || "",
  };
}

function routeFor(pathname) {
  return ROUTES.find((route) => route.path === normalizePath(pathname)) || ROUTES[0];
}

function AppShell() {
  const [locationState, setLocationState] = useState(currentLocationState());
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => window.localStorage.getItem("dashboard-sidebar") === "collapsed");
  const [commandCenterOpen, setCommandCenterOpen] = useState(() => window.localStorage.getItem("dashboard-command-center") === "open");
  const [focusModes, setFocusModes] = useState(() => loadStoredMap("dashboard-focus-modes"));
  const [selectedScenario, setSelectedScenario] = useState("");
  const [selectedSpec, setSelectedSpec] = useState("");
  const [jobs, setJobs] = useState({});
  const [refreshToken, setRefreshToken] = useState(0);
  const navRefs = useRef([]);
  const locationRef = useRef(locationState);
  const scrollPositionsRef = useRef({});
  const navigationModeRef = useRef("push");

  const { data: shellData, loading: shellLoading, error: shellError } = useCachedPageData(fetchShellData, {
    refreshToken,
    initialData: null,
    autoRefreshMs: 20000,
  });

  const currentRoute = useMemo(() => routeFor(locationState.pathname), [locationState.pathname]);
  const CurrentPage = currentRoute.component;
  const activeJob = Object.values(jobs).find((job) => job && ["queued", "running"].includes(job.status));
  const focusMode = Boolean(currentRoute.supportsFocusMode && focusModes[currentRoute.id]);
  const failureScenarios = useMemo(
    () => (shellData?.kpi?.available_scenarios || []).filter((scenario) => !String(scenario.path || "").endsWith("healthy.yaml")),
    [shellData?.kpi?.available_scenarios],
  );

  useEffect(() => {
    locationRef.current = locationState;
  }, [locationState]);

  useEffect(() => {
    function handlePopState() {
      scrollPositionsRef.current[locationKey(locationRef.current)] = window.scrollY;
      navigationModeRef.current = "restore";
      setLocationState(currentLocationState());
    }
    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => {
    window.localStorage.setItem("dashboard-sidebar", sidebarCollapsed ? "collapsed" : "expanded");
  }, [sidebarCollapsed]);

  useEffect(() => {
    window.localStorage.setItem("dashboard-command-center", commandCenterOpen ? "open" : "closed");
  }, [commandCenterOpen]);

  useEffect(() => {
    window.localStorage.setItem("dashboard-focus-modes", JSON.stringify(focusModes));
  }, [focusModes]);

  useEffect(() => {
    const nextMode = navigationModeRef.current;
    const nextKey = locationKey(locationState);
    window.requestAnimationFrame(() => {
      window.scrollTo({
        top: nextMode === "restore" ? scrollPositionsRef.current[nextKey] || 0 : 0,
        behavior: "auto",
      });
    });
    navigationModeRef.current = "push";
  }, [locationState]);

  useEffect(() => {
    if (!failureScenarios.length) {
      if (selectedScenario) {
        setSelectedScenario("");
      }
      return;
    }

    const stillValid = failureScenarios.some((scenario) => scenario.path === selectedScenario);
    if (!selectedScenario || !stillValid) {
      setSelectedScenario(preferredScenarioPath(failureScenarios, "week3_confidence_scale_break"));
    }
  }, [failureScenarios, selectedScenario]);

  useEffect(() => {
    if (!selectedSpec && shellData?.whatIf?.available_specs?.length) {
      setSelectedSpec(
        shellData.whatIf.available_specs.find((spec) => String(spec.id || spec.path).includes("confidence_scale_change"))?.path
        || shellData.whatIf.available_specs[0].path
        || shellData.whatIf.available_specs[0].id
        || "",
      );
    }
  }, [selectedSpec, shellData?.whatIf?.available_specs]);

  useEffect(() => {
    const queuedJobs = Object.values(jobs).filter((job) => job && ["queued", "running"].includes(job.status));
    if (!queuedJobs.length) {
      return undefined;
    }
    const intervalId = window.setInterval(async () => {
      const updates = await Promise.all(queuedJobs.map((job) => fetchJob(job.job_id)));
      for (const job of updates) {
        setJobs((current) => ({ ...current, [job.kind]: job }));
        if (job.status === "completed") {
          invalidateDashboardCache();
          setRefreshToken((current) => current + 1);
        }
      }
    }, 1500);
    return () => {
      window.clearInterval(intervalId);
    };
  }, [jobs]);

  function navigate(path, options = {}) {
    const { replace = false, restoreScroll = false } = options;
    scrollPositionsRef.current[locationKey(locationRef.current)] = window.scrollY;
    const nextUrl = new URL(path, window.location.origin);
    const nextPathname = normalizePath(nextUrl.pathname);
    const nextSearch = nextUrl.search || "";
    if (nextPathname === locationState.pathname && nextSearch === locationState.search) {
      return;
    }
    const serialized = `${nextPathname}${nextSearch}`;
    if (replace) {
      window.history.replaceState({}, "", serialized);
    } else {
      window.history.pushState({}, "", serialized);
    }
    navigationModeRef.current = restoreScroll ? "restore" : "push";
    setLocationState({ pathname: nextPathname, search: nextSearch });
  }

  async function queueJob(kind, task) {
    try {
      const job = await task();
      setJobs((current) => ({ ...current, [kind]: job }));
    } catch (error) {
      setJobs((current) => ({
        ...current,
        [kind]: {
          kind,
          status: "failed",
          error: error.message || `${kind} request failed.`,
        },
      }));
    }
  }

  function handleNavKeyDown(event, index, route) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      navRefs.current[(index + 1) % ROUTES.length]?.focus();
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      navRefs.current[(index - 1 + ROUTES.length) % ROUTES.length]?.focus();
      return;
    }
    if (event.key === "Home") {
      event.preventDefault();
      navRefs.current[0]?.focus();
      return;
    }
    if (event.key === "End") {
      event.preventDefault();
      navRefs.current[ROUTES.length - 1]?.focus();
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      navigate(route.path);
    }
  }

  return (
    <div className={`app-shell ${sidebarCollapsed ? "app-shell--collapsed" : ""} ${focusMode ? "app-shell--focus" : ""}`}>
      <aside className="app-sidebar">
        <div className="app-sidebar__header">
          <button
            className="sidebar-toggle"
            type="button"
            onClick={() => setSidebarCollapsed((current) => !current)}
            aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {sidebarCollapsed ? ">>" : "<<"}
          </button>
          {!sidebarCollapsed ? (
            <div>
              <p className="section-kicker">{PLATFORM_FULL_NAME}</p>
              <strong>{PLATFORM_SHORT_NAME}</strong>
            </div>
          ) : null}
        </div>

        <nav className="sidebar-nav" aria-label="Dashboard pages">
          {ROUTES.map((route, index) => {
            const active = currentRoute.id === route.id;
            return (
              <button
                key={route.id}
                ref={(element) => {
                  navRefs.current[index] = element;
                }}
                className={`sidebar-link ${active ? "sidebar-link--active" : ""}`}
                type="button"
                onClick={() => navigate(route.path)}
                onKeyDown={(event) => handleNavKeyDown(event, index, route)}
                aria-current={active ? "page" : undefined}
              >
                <span className="sidebar-link__badge">{route.shortLabel}</span>
                {!sidebarCollapsed ? <span>{route.label}</span> : null}
              </button>
            );
          })}
        </nav>
      </aside>

      <div className="app-main">
        <div className={`app-header-shell ${currentRoute.id === "lineage" ? "app-header-shell--lineage" : ""}`.trim()}>
          <GlobalUtilityBar
            shellData={shellData}
            shellLoading={shellLoading}
            activeJob={activeJob}
            onOpenCommandCenter={() => setCommandCenterOpen(true)}
          />
          <CompactPageHeader
            route={currentRoute}
            navigate={navigate}
            className={currentRoute.id === "lineage" ? "page-header--lineage" : ""}
            supplementalAction={
              currentRoute.supportsFocusMode
                ? {
                  label: focusMode ? "Exit focus mode" : "Enter focus mode",
                  onClick: () => setFocusModes((current) => ({ ...current, [currentRoute.id]: !current[currentRoute.id] })),
                }
                : null
            }
          />
        </div>

        {shellError ? <div className="app-shell-banner"><div className="error-banner">{shellError}</div></div> : null}
        {Object.values(jobs).some((job) => job?.status === "failed") ? (
          <div className="app-shell-banner">
            <div className="error-banner">
              {Object.values(jobs).find((job) => job?.status === "failed")?.error || "A background demo job failed."}
            </div>
          </div>
        ) : null}

        <main className={`app-content ${currentRoute.id === "lineage" ? "app-content--lineage" : ""}`.trim()}>
          <div className="page-stage" key={currentRoute.id}>
            <Suspense fallback={<div className="empty-state"><strong>Loading page…</strong></div>}>
              <CurrentPage refreshToken={refreshToken} navigate={navigate} locationSearch={locationState.search} focusMode={focusMode} />
            </Suspense>
          </div>
        </main>

        <CommandCenter
          open={commandCenterOpen}
          onClose={() => setCommandCenterOpen(false)}
          shellData={shellData}
          shellLoading={shellLoading}
          activeJob={activeJob}
          failureScenarios={failureScenarios}
          selectedScenario={selectedScenario}
          onScenarioChange={setSelectedScenario}
          onRestoreHealthy={() => queueJob("publish_real", () => publishMode("real"))}
          onRunFailureDemo={() => queueJob("regenerate", () => regenerateScenario(selectedScenario))}
          onInjectViolations={() => queueJob("publish_violated", () => publishMode("violated"))}
          selectedSpec={selectedSpec}
          onSpecChange={setSelectedSpec}
          onRunWhatIf={() => queueJob("what_if", () => runWhatIf(selectedSpec))}
        />
      </div>
    </div>
  );
}

export default AppShell;
