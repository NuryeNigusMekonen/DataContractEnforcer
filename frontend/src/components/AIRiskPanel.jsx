import { useState } from "react";

import { formatCompactNumber, formatPercent } from "../utils/formatters";
import { getStatusTone } from "../utils/status";

function RiskCard({ label, status, primary, secondary, tertiary }) {
  return (
    <article className="mini-stat-card">
      <div className="mini-stat-top">
        <strong>{label}</strong>
        <span className={`badge badge--${getStatusTone(status)}`}>{status || "UNKNOWN"}</span>
      </div>
      <p>{primary}</p>
      <p className="muted-copy">{secondary}</p>
      {tertiary ? <p className="muted-copy">{tertiary}</p> : null}
    </article>
  );
}

function answerForSemanticConsistency(embedding) {
  const status = String(embedding.status || "UNKNOWN").toUpperCase();
  if (status === "PASS") {
    return "Yes. The text going into the AI system is semantically consistent with the current baseline.";
  }
  if (status === "FAIL") {
    return "No. The text going into the AI system has drifted semantically beyond the accepted threshold.";
  }
  return "Semantic consistency could not be determined from the current run.";
}

function answerForPromptInputs(prompt) {
  const status = String(prompt.status || "UNKNOWN").toUpperCase();
  if (status === "PASS") {
    return "Yes. Prompt inputs are valid for the current run.";
  }
  if (status === "WARN") {
    return `Partially. ${formatCompactNumber(prompt.quarantined_records)} prompt inputs were quarantined and require review.`;
  }
  if (status === "FAIL") {
    return "No. Prompt inputs failed validation and require correction before they can be trusted.";
  }
  return "Prompt-input validity could not be determined from the current run.";
}

function answerForStructuredOutputs(structuredOutput) {
  const status = String(structuredOutput.status || "UNKNOWN").toUpperCase();
  if (status === "PASS") {
    return "Yes. Structured outputs are valid against the expected schema.";
  }
  if (status === "WARN") {
    return `Partially. ${formatCompactNumber(structuredOutput.schema_violations)} outputs violated the schema and need review.`;
  }
  if (status === "FAIL") {
    return `No. Structured outputs are not valid: ${formatCompactNumber(structuredOutput.schema_violations)} outputs failed schema enforcement.`;
  }
  return "Structured-output validity could not be determined from the current run.";
}

function answerForTraceContracts(traceContracts) {
  const status = String(traceContracts.status || "UNKNOWN").toUpperCase();
  if (status === "PASS") {
    return "Yes. Trace records are valid enough to trust monitoring and debugging.";
  }
  if (status === "WARN") {
    return `Partially. Trace contracts raised warnings and should be reviewed before relying on the telemetry fully.`;
  }
  if (status === "FAIL") {
    return `No. Trace records are not currently trustworthy: ${formatCompactNumber(traceContracts.schema_invalid_records)} rows failed trace validation.`;
  }
  return "Trace validity could not be determined from the current run.";
}

function AIRiskPanel({ aiRisk }) {
  const embedding = aiRisk.embedding_drift || {};
  const prompt = aiRisk.prompt_input_validation || {};
  const structuredOutput = aiRisk.structured_llm_output_enforcement || aiRisk.llm_output_schema_rate || {};
  const traceContracts = aiRisk.langsmith_trace_schema_contracts || {};
  const answerItems = [
    {
      id: "semantic",
      tabLabel: "Semantic",
      question: "Is the text going into the AI system still semantically consistent?",
      status: embedding.status,
      answer: answerForSemanticConsistency(embedding),
      detail: `Drift score ${embedding.drift_score ?? "--"} vs threshold ${embedding.threshold ?? "--"}.`,
    },
    {
      id: "prompt",
      tabLabel: "Prompts",
      question: "Are prompt inputs valid?",
      status: prompt.status,
      answer: answerForPromptInputs(prompt),
      detail: `${formatCompactNumber(prompt.valid_records)} valid inputs and ${formatCompactNumber(prompt.quarantined_records)} quarantined.`,
    },
    {
      id: "output",
      tabLabel: "Outputs",
      question: "Are structured outputs valid?",
      status: structuredOutput.status,
      answer: answerForStructuredOutputs(structuredOutput),
      detail: `${formatCompactNumber(structuredOutput.schema_violations)} schema violations out of ${formatCompactNumber(structuredOutput.total_outputs)} outputs.`,
    },
    {
      id: "traces",
      tabLabel: "Traces",
      question: "Are trace records valid enough to trust monitoring and debugging?",
      status: traceContracts.status,
      answer: answerForTraceContracts(traceContracts),
      detail: `${formatCompactNumber(traceContracts.schema_invalid_records)} schema-invalid rows and ${formatCompactNumber(traceContracts.failed_contract_checks)} failing trace checks.`,
    },
  ];
  const initialSelectedAnswer =
    answerItems.find((item) => String(item.status || "UNKNOWN").toUpperCase() !== "PASS")?.id || answerItems[0].id;
  const [selectedAnswerId, setSelectedAnswerId] = useState(initialSelectedAnswer);
  const selectedAnswer = answerItems.find((item) => item.id === selectedAnswerId) || answerItems[0];

  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">AI Contract Extensions</p>
          <h2>Embedding, prompt, output, and trace risk checks</h2>
        </div>
      </div>

      <div className="ai-risk-layout">
        <div className="ai-risk-column">
          <div className="panel-heading panel-heading--tight">
            <div>
              <p className="section-kicker">Risk Checks</p>
              <h2>Current contract status</h2>
            </div>
          </div>

          <div className="ai-risk-check-grid">
            <RiskCard
              label="Embedding drift"
              status={embedding.status}
              primary={`Drift score ${embedding.drift_score ?? "--"} against threshold ${embedding.threshold ?? "--"}`}
              secondary={embedding.interpretation || "Semantic stability assessment unavailable."}
            />
            <RiskCard
              label="Prompt input validation"
              status={prompt.status}
              primary={`${formatCompactNumber(prompt.valid_records)} valid inputs • ${formatCompactNumber(prompt.quarantined_records)} quarantined`}
              secondary={prompt.quarantine_path || "No quarantine path reported."}
            />
            <RiskCard
              label="Structured LLM output"
              status={structuredOutput.status}
              primary={`${formatCompactNumber(structuredOutput.schema_violations)} schema violations out of ${formatCompactNumber(structuredOutput.total_outputs)} outputs`}
              secondary={`Violation rate ${formatPercent(structuredOutput.violation_rate || 0)} • baseline ${formatPercent(structuredOutput.baseline_violation_rate || 0)}`}
              tertiary={`Trend ${structuredOutput.trend || "unknown"} • warn ${formatPercent(structuredOutput.warn_threshold || 0)} • fail ${formatPercent(structuredOutput.fail_threshold || 0)}`}
            />
            <RiskCard
              label="LangSmith trace contracts"
              status={traceContracts.status}
              primary={`${formatCompactNumber(traceContracts.failed_contract_checks)} failing checks across ${formatCompactNumber(traceContracts.total_contract_checks)} contract clauses`}
              secondary={`${formatCompactNumber(traceContracts.schema_invalid_records)} schema-invalid rows out of ${formatCompactNumber(traceContracts.total_records)} traces`}
              tertiary={(traceContracts.failing_check_ids || []).slice(0, 2).join(" • ") || "No failing trace checks reported."}
            />
          </div>
        </div>

        <div className="ai-risk-column ai-risk-column--answers">
          <div className="panel-heading panel-heading--tight">
            <div>
              <p className="section-kicker">Direct Answers</p>
              <h2>What the current AI risk status means</h2>
            </div>
          </div>

          <div className="tab-row ai-answer-tabs">
            {answerItems.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`tab-chip ${item.id === selectedAnswer.id ? "tab-chip--active" : ""}`}
                onClick={() => setSelectedAnswerId(item.id)}
              >
                <span>{item.tabLabel}</span>
                <span className={`badge badge--${getStatusTone(item.status)}`}>{item.status || "UNKNOWN"}</span>
              </button>
            ))}
          </div>

          <article className="ai-answer-card">
            <div className="ai-answer-card__top">
              <strong>{selectedAnswer.question}</strong>
              <span className={`badge badge--${getStatusTone(selectedAnswer.status)}`}>{selectedAnswer.status || "UNKNOWN"}</span>
            </div>
            <p className="ai-answer-card__summary">{selectedAnswer.answer}</p>
            <p className="muted-copy">{selectedAnswer.detail}</p>
          </article>
        </div>
      </div>
    </section>
  );
}

export default AIRiskPanel;
