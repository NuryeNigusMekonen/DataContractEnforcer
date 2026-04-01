# Shared Brain

## Architecture Rules

Select an intent before any write-like tool.
Only edit files inside `owned_scope` for the active intent.
Include `intent_id`, `mutation_class`, and requirement refs on every write payload.
Keep `agent_trace.jsonl` append-only and emit one JSON object per successful write-like tool call.
Run the command in `acceptance_criteria` before moving an intent to completed.

## Verification Rules

Write verification outcomes to `.orchestration/verification_log.jsonl`.
If a verification step fails, append one lesson and retry only after a fresh read.
If a write is blocked as stale, record the mismatch in `.orchestration/stale_write_blocks.jsonl`.

## Lessons Learned

2026-02-19: preload the latest intent trace context before follow-up refactors so later edits keep the same lineage.
2026-02-20: stale write errors must include the current disk hash and a reread hint or builders will retry blindly.
2026-02-20: trace writer optimizations are safe only if they preserve append order and never rewrite existing JSONL lines.

## Decisions

Store orchestration state in `.orchestration`.
Use `content_hash` for spatial independence when code moves.
Classify new product behavior as `INTENT_EVOLUTION` and schema-neutral cleanup as `AST_REFACTOR`.
