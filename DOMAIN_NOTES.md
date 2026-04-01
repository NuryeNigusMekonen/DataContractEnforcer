# DOMAIN_NOTES

This workspace began as a docs-only repository, but the current examples now use canonical JSONL outputs rebuilt from the real artifacts under `artifacts/week1` through `artifacts/week5`. The week alignment is: `.orchestration` intent records from week 1, Digital Courtroom verdict structure from week 2, provenance-heavy document extraction from week 3, cartography lineage from week 4, and Apex Ledger loan lifecycle events from week 5.

## 1. Backward-compatible vs. breaking schema change

Backward-compatible changes preserve existing producer and consumer expectations.

- Compatible: add an optional field to `outputs/week3/extractions.jsonl`, such as `reviewed_by`, while keeping all existing keys unchanged.
- Compatible: add a new enum value to `outputs/week2/verdicts.jsonl` for a non-required metadata field, while keeping `overall_verdict` unchanged.
- Compatible: add a nullable property to an event payload schema in `schemas/events/DocumentProcessed-1.0.json` without changing existing required properties.

Breaking changes invalidate an assumption held by an existing consumer or contract.

- Breaking: change `extracted_facts.confidence` in `outputs/week3/extractions.jsonl` from a float on `0.0-1.0` to a number on `0-100`.
- Breaking: remove `event_type` or change it from PascalCase in `outputs/week5/events.jsonl`, which breaks event schema lookup.
- Breaking: remove `edges.relationship` enum values or rename them in `outputs/week4/lineage_snapshots.jsonl`, which breaks lineage interpretation.

## 2. Week 3 confidence scale change and the Week 4 failure

Measured confidence distribution from the clean real week 3 extraction set:

```text
count=208 min=0.050 max=0.920 mean=0.839
```

If that field changes to `83.0`, `87.0`, `91.0`, and `95.0`, the scale changes but the values still look numeric. That is the dangerous case: the consumer keeps running.

Failure path:

1. Week 3 emits `extracted_facts.confidence` on the wrong scale.
2. Week 4 consumes the extraction dataset and treats confidence as a normalized weight.
3. Any downstream logic that ranks or thresholds on confidence receives inflated values.
4. Week 7 catches the issue through both a range failure and a statistical drift failure before the bad data is trusted.

Bitol-style clause that catches the change:

```yaml
apiVersion: v1
kind: DataContract
id: week3-document-refinery-extractions
dataset:
  name: week3_extractions
fields:
  extracted_facts.confidence:
    type: number
    required: true
    minimum: 0.0
    maximum: 1.0
    description: Confidence must remain normalized to a 0.0-1.0 scale.
quality:
  - type: range
    field: extracted_facts.confidence
    must_be_between: [0.0, 1.0]
```

## 3. How the Enforcer uses lineage for blame chains

The lineage graph in `outputs/week4/lineage_snapshots.jsonl` contains a dataset node for `outputs/week3/extractions.jsonl` and downstream pipeline nodes for the cartographer and the enforcer. The attribution flow is:

1. The runner emits a structured failure such as `extracted_facts.confidence.range`.
2. The attributor maps the failing field to a likely source system using the field name and lineage node metadata.
3. It finds the candidate source file `outputs/week3/extractions.jsonl`.
4. It reads downstream entries from the generated contract lineage block rather than recomputing blast radius.
5. It writes `violation_log/violations.jsonl` with candidate files, affected nodes, and estimated failing records.
6. If Git history exists, the same step also attaches recent commits for those candidate files. In this workspace Git history is unavailable, so the log records that explicitly.

The graph traversal logic is intentionally simple in this MVP: locate nodes whose IDs or paths match the failing field's owning dataset, then use the contract's downstream lineage list as the blast-radius view.

## 4. LangSmith trace contract with structural, statistical, and AI-specific clauses

```yaml
apiVersion: v1
kind: DataContract
id: langsmith-trace-records
dataset:
  name: trace_record
fields:
  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
  start_time:
    type: string
    required: true
    format: date-time
  end_time:
    type: string
    required: true
    format: date-time
  total_cost:
    type: number
    required: true
    minimum: 0.0
quality:
  - type: rule
    field: end_time
    assertion: end_time > start_time
  - type: statistical_drift
    field: total_tokens
    threshold_stddev: 3
ai_extensions:
  - type: output_schema_rate
    field: overall_verdict
    warn_threshold: 0.02
```

Structural clause:
`run_type` must be one of the five allowed values.

Statistical clause:
`total_tokens` drift beyond three standard deviations is a failure.

AI-specific clause:
output schema violation rate is tracked as a separate LLM risk measure.

## 5. Why contracts get stale and how this architecture resists it

The most common production failure mode is stale contracts: teams change a producer, the contract is never regenerated, and validation slowly stops representing reality. Contracts get stale because they are treated as documentation instead of executable gates.

This architecture resists that in four ways:

- Contract generation is scriptable from live JSONL data, so regeneration is cheap.
- Validation writes machine-readable reports, so failures are visible in CI or orchestration rather than buried in prose.
- Snapshot history under `schema_snapshots/` turns schema changes into diffable artifacts.
- The injected-violation workflow proves the system can detect both structural and statistical problems, not just renamed fields.

In this repo the clearest example is the week 3 confidence field. The clean contract captures `maximum: 1.0`, the violated snapshot captures `maximum: 95.0`, and the schema analyzer classifies that change as breaking instead of letting the drift become the new normal.

The practical lesson is that contract freshness is an operational discipline, not a documentation task. If regeneration, validation, attribution, and reporting are all one command away, stale contracts become visible work instead of hidden risk.
