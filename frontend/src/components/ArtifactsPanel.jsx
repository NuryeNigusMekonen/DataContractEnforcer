import { useEffect, useMemo, useState } from "react";

import { formatCompactNumber, formatTimestamp } from "../utils/formatters";
import { getStatusTone } from "../utils/status";
import { PLATFORM_SHORT_NAME, getSystemDisplayName, replaceSystemNames } from "../utils/systemNames";

function basename(value) {
  return String(value || "").split("/").pop() || "";
}

function extensionOf(value) {
  const name = basename(value);
  const parts = name.split(".");
  return parts.length > 1 ? parts.pop().toUpperCase() : "FILE";
}

function humanizeToken(value) {
  return String(value || "")
    .replace(/\.[^.]+$/, "")
    .replace(/(_dbt|_baseline|-baseline|_records|-records)/gi, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function artifactKindLabel(item) {
  const path = String(item?.path || item?.name || "").toLowerCase();
  if (path.includes("validation_reports")) {
    return "Validation report";
  }
  if (path.includes("schema_snapshot") || path.includes("schema_snapshots") || path.includes("snapshot")) {
    return "Schema snapshot";
  }
  if (path.includes("violation_log") || path.includes("violations")) {
    return "Violation log";
  }
  if (path.includes("report_data") || path.includes("report.md") || path.includes("enforcer_report")) {
    return "Platform report";
  }
  if (item?.contract_id) {
    return "Contract schema";
  }
  if (item?.kind) {
    return humanizeToken(item.kind);
  }
  return "Artifact";
}

function artifactSystemName(item, short = true) {
  const candidate = item?.title || item?.contract_id || item?.dataset || item?.path || item?.name || "";
  const display = getSystemDisplayName(candidate, { short, fallback: "" });
  return display || "";
}

function artifactPrimaryName(item) {
  const systemName = artifactSystemName(item, true);
  if (systemName) {
    return systemName;
  }
  const fileName = basename(item?.name || item?.path);
  if (String(fileName).toLowerCase().includes("report_data")) {
    return PLATFORM_SHORT_NAME;
  }
  return humanizeToken(fileName || item?.title || "Artifact");
}

function artifactSubtitle(item) {
  const systemName = artifactSystemName(item, true);
  const subtitleParts = [];
  if (systemName) {
    subtitleParts.push(systemName);
  }
  subtitleParts.push(artifactKindLabel(item));
  subtitleParts.push(extensionOf(item?.path || item?.name));
  return subtitleParts.filter(Boolean).join(" • ");
}

function artifactMatches(item, query) {
  if (!query) {
    return true;
  }
  return [
    item.name,
    item.path,
    item.contract_id,
    item.dataset,
    item.status,
    item.kind,
    item.title,
    item.latest_snapshot,
    replaceSystemNames(item.name),
    replaceSystemNames(item.path),
    replaceSystemNames(item.contract_id),
    artifactPrimaryName(item),
    artifactSubtitle(item),
  ]
    .filter(Boolean)
    .some((value) => String(value).toLowerCase().includes(query));
}

function artifactBadge(item) {
  if (!item) {
    return { label: "Preview", tone: "neutral" };
  }
  if (item.status) {
    return {
      label: item.status,
      tone: getStatusTone(item.status),
    };
  }
  if (item.kind) {
    return {
      label: String(item.kind).replace(/_/g, " "),
      tone: "neutral",
    };
  }
  return {
    label: item.contract_id ? "Contract" : "Artifact",
    tone: "neutral",
  };
}

function buildArtifactFacts(item) {
  if (!item) {
    return [];
  }

  return [
    { label: "Path", value: item.path || "n/a" },
    { label: "Updated", value: formatTimestamp(item.updated_at) },
    { label: "Contract ID", value: item.contract_id || "n/a" },
    { label: "Dataset", value: item.dataset || "n/a" },
    { label: "Checks", value: item.total_checks ?? item.result_count ?? item.snapshot_count ?? item.record_count ?? "n/a" },
    { label: "Status", value: item.status || artifactBadge(item).label },
  ].filter((fact) => fact.value !== "n/a" || ["Path", "Updated", "Contract ID"].includes(fact.label));
}

function relatedArtifactLinks(item) {
  if (!item) {
    return [];
  }

  const path = String(item.path || "").toLowerCase();
  const links = [];
  if (path.includes("violations") || path.includes("violation_log")) {
    links.push({ label: "Open violations", path: "/violations" });
  }
  if (path.includes("schema") || path.includes("snapshot")) {
    links.push({ label: "Open schema evolution", path: "/schema-evolution" });
  }
  if (item.contract_id || path.includes("validation_reports") || item.status) {
    links.push({ label: "Open validation", path: "/validation" });
  }
  return links.filter((link, index, all) => all.findIndex((candidate) => candidate.path === link.path) === index);
}

function artifactTypeLabel(item) {
  return artifactKindLabel(item).toLowerCase();
}

function copyableFact(label) {
  return label === "Path" || label === "Contract ID";
}

function ArtifactList({ title, items = [], selectedPath, onSelect }) {
  return (
    <article className="artifact-column">
      <div className="artifact-column-head">
        <strong>{title}</strong>
        <span>{formatCompactNumber(items.length)}</span>
      </div>

      <div className="artifact-list">
        {items.map((item) => (
          <button
            className={`artifact-row ${selectedPath === item.path ? "artifact-row--selected" : ""}`}
            key={item.path}
            type="button"
            onClick={() => onSelect(item)}
          >
            <span className={`artifact-row__indicator artifact-row__indicator--${artifactBadge(item).tone}`} aria-hidden="true" />
            <div className="artifact-row__copy">
              <strong title={artifactPrimaryName(item)}>{artifactPrimaryName(item)}</strong>
              <small title={artifactSubtitle(item)}>{artifactSubtitle(item)}</small>
            </div>
            <div className="artifact-row__meta">
              <small>{artifactTypeLabel(item)}</small>
              <small>{formatTimestamp(item.updated_at)}</small>
            </div>
          </button>
        ))}

        {!items.length ? (
          <div className="empty-state empty-state--compact">
            <strong>No matching files</strong>
            <p className="muted-copy">Broaden the search to bring contracts, reports, or snapshots back into view.</p>
          </div>
        ) : null}
      </div>
    </article>
  );
}

function ClausePreview({ clauses = [] }) {
  if (!clauses.length) {
    return (
      <div className="empty-state empty-state--compact">
        <strong>No clause preview is available.</strong>
      </div>
    );
  }

  return (
    <div className="stack-list">
      {clauses.map((clause) => (
        <article className="clause-card" key={`${clause.contract_id || clause.contract_name}-${clause.id}`}>
          <div className="list-card-top">
            <strong className="clause-card__title">{clause.id}</strong>
            <span className={`badge badge--${getStatusTone(clause.severity)}`}>{clause.severity}</span>
          </div>
          <p>{clause.description}</p>
          <pre className="code-preview code-preview--compact">{clause.rule_summary}</pre>
          <p className="muted-copy">{getSystemDisplayName(clause.contract_name || clause.contract_id, { fallback: replaceSystemNames(clause.contract_name || clause.contract_id) })}</p>
        </article>
      ))}
    </div>
  );
}

function ArtifactsPanel({ artifacts, onNavigate }) {
  const [selectedArtifactPath, setSelectedArtifactPath] = useState(() => window.localStorage.getItem("dashboard-selected-artifact") || "");
  const [search, setSearch] = useState("");
  const [copiedField, setCopiedField] = useState("");

  const query = search.trim().toLowerCase();
  const contractItems = artifacts.contracts || [];
  const reportItems = useMemo(
    () => [...(artifacts.validation_reports || []), ...(artifacts.report_files || []), ...(artifacts.violation_logs || [])],
    [artifacts],
  );
  const snapshotItems = artifacts.schema_snapshots || [];

  const filteredContracts = useMemo(() => contractItems.filter((item) => artifactMatches(item, query)), [contractItems, query]);
  const filteredReports = useMemo(() => reportItems.filter((item) => artifactMatches(item, query)), [reportItems, query]);
  const filteredSnapshots = useMemo(() => snapshotItems.filter((item) => artifactMatches(item, query)), [snapshotItems, query]);

  const visibleItems = [...filteredContracts, ...filteredReports, ...filteredSnapshots];
  const displayArtifact = visibleItems.find((item) => item.path === selectedArtifactPath) || visibleItems[0] || null;
  const badge = artifactBadge(displayArtifact);
  const artifactLinks = relatedArtifactLinks(displayArtifact);
  const metadataFacts = buildArtifactFacts(displayArtifact);

  useEffect(() => {
    if (!visibleItems.length) {
      if (selectedArtifactPath) {
        setSelectedArtifactPath("");
      }
      return;
    }
    const nextPath = visibleItems.some((item) => item.path === selectedArtifactPath) ? selectedArtifactPath : visibleItems[0].path;
    if (nextPath !== selectedArtifactPath) {
      setSelectedArtifactPath(nextPath);
    }
  }, [selectedArtifactPath, visibleItems]);

  useEffect(() => {
    if (selectedArtifactPath) {
      window.localStorage.setItem("dashboard-selected-artifact", selectedArtifactPath);
    }
  }, [selectedArtifactPath]);

  const clauses = useMemo(() => {
    if (displayArtifact?.clause_preview?.length) {
      return displayArtifact.clause_preview.map((clause) => ({
        ...clause,
        contract_id: displayArtifact.contract_id,
        contract_name: displayArtifact.title || displayArtifact.contract_id,
      }));
    }
    if (displayArtifact?.contract_id) {
      const matching = (artifacts.sample_contract_clauses || []).filter((clause) => clause.contract_id === displayArtifact.contract_id);
      if (matching.length) {
        return matching;
      }
    }
    return (artifacts.sample_contract_clauses || []).slice(0, 6);
  }, [artifacts.sample_contract_clauses, displayArtifact]);

  async function handleCopy(label, value) {
    if (!value) {
      return;
    }
    try {
      await navigator.clipboard.writeText(String(value));
      setCopiedField(label);
      window.setTimeout(() => setCopiedField((current) => (current === label ? "" : current)), 1200);
    } catch {
      setCopiedField("");
    }
  }

  if (!contractItems.length && !reportItems.length && !snapshotItems.length) {
    return (
      <section className="panel artifact-panel">
        <div className="empty-state empty-state--workspace">
          <strong>No artifacts are available yet.</strong>
          <p className="muted-copy">Publish real or violated mode from the command center to populate contracts, reports, and snapshots for review.</p>
        </div>
      </section>
    );
  }

  return (
    <section className="panel artifact-panel">
      <div className="panel-heading artifact-panel__header">
        <div>
          <p className="section-kicker">Artifacts Workspace</p>
          <h2>Browse contracts, reports, snapshots, and logs with a denser review flow</h2>
          <p className="muted-copy">The top of the page stays light so the file lists and preview area can take over the screen.</p>
        </div>

        <div className="artifact-toolbar">
          <label className="field field--grow">
            <span>Search artifacts</span>
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="File name, contract id, dataset, or path"
            />
          </label>
        </div>
      </div>

      <div className="artifact-workspace">
        <div className="artifact-grid">
          <ArtifactList
            title="Contracts"
            items={filteredContracts}
            selectedPath={displayArtifact?.path}
            onSelect={(item) => setSelectedArtifactPath(item.path)}
          />
          <ArtifactList
            title="Reports and logs"
            items={filteredReports}
            selectedPath={displayArtifact?.path}
            onSelect={(item) => setSelectedArtifactPath(item.path)}
          />
          <ArtifactList
            title="Snapshots"
            items={filteredSnapshots}
            selectedPath={displayArtifact?.path}
            onSelect={(item) => setSelectedArtifactPath(item.path)}
          />
        </div>

        <div className="artifact-preview-stack">
          <article className="artifact-preview-card">
            <div className="list-card-top">
              <div className="artifact-preview-heading">
                <strong>{displayArtifact ? artifactPrimaryName(displayArtifact) : "No artifact selected"}</strong>
                <p className="muted-copy" title={displayArtifact?.path || "Search or pick a file to preview its contents."}>
                  {displayArtifact ? artifactSubtitle(displayArtifact) : "Search or pick a file to preview its contents."}
                </p>
              </div>
              <div className="artifact-preview-actions">
                <span className={`badge badge--${badge.tone}`}>{badge.label}</span>
                {artifactLinks.map((link) => (
                  <button key={link.path} className="text-button" type="button" onClick={() => onNavigate?.(link.path)}>
                    {link.label}
                  </button>
                ))}
              </div>
            </div>

            <section className="detail-section">
              <div className="section-rule-heading">
                <span className="micro-label">Metadata</span>
              </div>
              <div className="artifact-metadata-grid">
                {metadataFacts.map((fact) => (
                  <article className="inline-fact inline-fact--artifact" key={fact.label}>
                    <span className="micro-label">{fact.label}</span>
                    <div className="inline-fact__value-row">
                      <strong className="value-ellipsis" title={String(fact.value || "")}>{String(fact.value || "")}</strong>
                      {copyableFact(fact.label) ? (
                        <button
                          className="icon-button"
                          type="button"
                          title={`Copy ${fact.label.toLowerCase()}`}
                          aria-label={`Copy ${fact.label.toLowerCase()}`}
                          onClick={() => handleCopy(fact.label, fact.value)}
                        >
                          {copiedField === fact.label ? "Done" : "Copy"}
                        </button>
                      ) : null}
                    </div>
                  </article>
                ))}
              </div>
            </section>

            <section className="detail-section">
              <div className="section-rule-heading">
                <span className="micro-label">Content preview</span>
              </div>
              <pre className="code-preview code-preview--tall">
                {displayArtifact?.preview || "Select an artifact from the lists to preview its current contents."}
              </pre>
            </section>
          </article>

          <article className="artifact-preview-sidecard" key={`${displayArtifact?.path || "empty"}-clauses`}>
            <div className="list-card-top">
              <strong>Contract clause preview</strong>
              <span>{formatCompactNumber(clauses.length)}</span>
            </div>
            <ClausePreview clauses={clauses} />
          </article>
        </div>
      </div>
    </section>
  );
}

export default ArtifactsPanel;
