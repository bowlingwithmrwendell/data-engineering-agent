# Framework mapping

This agent implements the five-layer *Data Engineering Agent Framework* (Wendell Killette, Agile by Killette).

| Framework layer | Core function | Where it lives |
|---|---|---|
| 1. Interaction Layer | Chat, dashboards, API endpoints, natural-language input | `app/static/index.html` (console), `app/static/governance.html` (scorecard), `app/main.py` (REST + OpenAPI), `app/agent.py` (plain English) |
| 2. Orchestration Engine | Intent parsing, task decomposition, workflow state, error recovery | `app/agent.py` (intent → spec), `app/orchestration.py` (state machine, workflow steps, `apply_patch` fixes), `app/spec.py` (typed contract + checks) |
| 3. Tool Calling Capabilities | Platform connectors, Git, CI/CD, deployment | `app/connectors/` (Databricks, Snowflake, Fabric), `app/compiler.py` (code + schedule artifacts), deploy log records the Git change set and CI re-check |
| 4. Code Sandbox | Isolated execution of SQL / PySpark / dbt, testing, quality checks | `app/sandbox.py` (isolated in-memory execution, time limit, quality + compliance checks) |
| 5. Information Layer | Memory, metadata catalog, lineage, compliance | `app/information.py`, `app/models.py` (catalog, lineage with column maps, runs, validations, governance tags), `app/seed.py` |

Governance spine shared with the PMO Delivery Agent: `app/governance.py` (review gate, limiter, kill switch, AI-call audit), `app/actions.py` (human approval of deployments), `app/grounding.py`.
