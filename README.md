# Data Engineering Agent

**Governed data pipelines on Databricks, Snowflake and Microsoft Fabric, built by people who don't write code and trusted by people who do.**

Describe the table you need in plain English, or build it step by step. The agent finds the right data in the catalog, generates the code for your platform, runs it in an isolated sandbox, and explains in plain English what it found: duplicate keys that would overstate totals, orphan records, personal data that must be masked, stale sources. A named human approves every deployment.

The AI never writes code freehand. It fills in a typed **pipeline spec**; a deterministic compiler turns that spec into Databricks SQL, Snowflake SQL, Fabric T-SQL, PySpark and dbt. What was tested is exactly what deploys.

> Built by **Wendell Killette — [Agile by Killette](https://agilebykillette.com)**. #4 in the series after the [Equity Compensation Management Agent](https://github.com/bowlingwithmrwendell/equity-comp-agent) and the [PMO Delivery Agent](https://github.com/bowlingwithmrwendell/pmo-delivery-agent). All data is fictional.

---

## Who it's for

| If you are… | You use… | You get… |
|---|---|---|
| An analyst, manager or business user | **Ask in plain English**, or the **step-by-step builder** (no AI needed) | A tested table, a plain-English summary of what it contains, and a checklist of anything you should know, each with a one-click **Apply fix** |
| A data engineer or developer | **Developer view** (one switch), the spec JSON, the REST API (`/docs`) | The generated SQL / PySpark / dbt / schedule files, how every check was measured, column-level lineage, and a clean connector interface to extend |
| An approver or data owner | **Approvals** | What will deploy, where, what the sandbox found, and the exact files that will be committed. Approve, or reject with a reason |

---

## The five-layer framework, as built

| Layer | What it does here | Code |
|---|---|---|
| **1. Interaction** | Web console (guided builder, plain-English chat, catalog browser, approvals), REST API with OpenAPI docs | `app/main.py`, `app/static/` |
| **2. Orchestration engine** | Turns intent into a spec, decomposes the work into steps, runs the state machine (draft → tested → pending approval → deployed), and recovers from errors with machine-applicable fixes | `app/orchestration.py`, `app/agent.py`, `app/spec.py` |
| **3. Tool calling** | One connector interface; simulated Databricks, Snowflake and Fabric connectors that name tables the way each platform does and emit its scheduling artifact (Asset Bundle job, Snowflake Task, Data Factory pipeline). Git change sets and CI re-checks are recorded on deploy | `app/connectors/`, `app/compiler.py` |
| **4. Code sandbox** | Executes the compiled code in a fresh, isolated in-memory database with only the pipeline's sample data and a hard time limit, then runs data-quality and compliance checks | `app/sandbox.py` |
| **5. Information** | Metadata catalog (friendly names, descriptions, owners, freshness, PII / Confidential tags), dataset and column-level lineage, run history, governance tags inherited by published tables | `app/information.py`, `app/models.py` |

---

## What the sandbox checks

Every check returns a plain-English verdict, a suggested fix, and `explanation: {formula, inputs, reasoning}`.

| Check | Fails or warns when… | Typical fix (one click) |
|---|---|---|
| Runs successfully | The compiled code errors or exceeds the time limit | — |
| Join doesn't duplicate rows | A lookup table has repeated keys, so a join multiplies rows and **overstates totals** (fails if totals are affected) | Switch to the certified one-row-per-key table |
| Every row finds its match | Keys have no partner (orphans) or are blank | Keep every row (left join) so nothing is silently dropped |
| Personal information (PII) | A PII column would leave the source unprotected | Mask it with a one-way hash (still matchable) |
| One row per grain | The breakdown columns don't identify a single row | — |
| Breakdown always filled in | Group-by values are blank | Ask the owner, or filter blanks |
| Source is up to date | A source was loaded longer ago than the pipeline's schedule allows | Ask the named owner |
| Design rules | Unknown columns (with "did you mean"), totalling text, misspelled filter values, raw table when a certified one exists, reading across platforms, name collisions | Rename, swap, or correct automatically |

The demo data has these problems planted on purpose, so every example shows something real.

---

## The governance I applied

| Choice | What it means in this build |
|---|---|
| **No freehand code** | Claude produces a typed spec; the compiler generates all code. Same spec, byte-identical code. |
| **Bounded authority** | Claude can read the catalog, create and test **drafts**, apply suggested fixes and *request* deployment. It has no tool to deploy, approve, change the catalog or run arbitrary SQL. |
| **Tested version only** | Only the exact spec version (hash) that passed the sandbox can be requested. Any change returns the pipeline to draft and supersedes a pending request. |
| **Re-test at approval** | Approval re-runs the sandbox on that exact spec. If anything fails now, nothing deploys (fails closed). |
| **Separation of duties** | The requester can't approve their own deployment. The AI, agents, bots and automated systems are refused as approvers and reviewers. |
| **PII protection** | PII can only leave a source masked or counted. Catalog and sandbox previews never show PII values, to people or to the model. |
| **Always human-validated** | Every AI answer is stored `PENDING_REVIEW`; a named human approves, edits then approves, or rejects it with a reason. Numbers not found in tool results are flagged, and an ungrounded answer can't be approved unchanged. |
| **Audit trail** | Every draft, test, fix, tool call, request, decision and deployment is in an append-only audit log. Every AI call is logged with who (salted hash), when, model, tools, tokens and outcome. Run history is append-only too. |
| **Limiter and kill switch** | Per-visitor rate limit and daily cap, global daily cap and token budget, `max_tokens`, tool-turn cap, request and question size limits — all in `.env`. `AI_ENABLED=false` or the runtime switch on `/governance` turns AI off; everything else keeps working. |
| **Governance scorecard** | `/governance` shows the model card, data policy, deployment and review gates, limits, the kill switch, live % human-validated and the AI call log. |

---

## A pipeline spec

```json
{
  "name": "Monthly revenue by region",
  "platform": "snowflake",
  "source": "orders",
  "joins":    [{"dataset": "customers", "left": "orders.customer_id", "right": "customers.customer_id", "how": "left"}],
  "filters":  [{"column": "orders.status", "op": "=", "value": "completed"}],
  "group_by": [{"column": "customers.region"}, {"column": "orders.order_date", "grain": "month"}],
  "measures": [{"agg": "sum", "column": "orders.amount", "name": "revenue"},
               {"agg": "count", "column": null, "name": "orders"}],
  "masks":    [],
  "target":   {"name": "monthly_revenue_by_region", "layer": "gold", "materialization": "table"},
  "schedule": "daily"
}
```

From that, the compiler writes for the chosen platform:

| File | Contents |
|---|---|
| `sql/<name>.sql` | Platform DDL + SELECT (Databricks SQL / Snowflake SQL / Fabric Warehouse T-SQL) |
| `notebooks/<name>.py` | PySpark (Databricks and Fabric Lakehouse) |
| `dbt/models/<layer>/<name>.sql` + `.yml` | dbt model with cross-database macros, and tests that follow the grain |
| `deploy/…` | `databricks.yml` job, Snowflake `task.sql`, or Fabric `pipeline.json` |
| `specs/<name>.json` | The spec: the source of truth |
| `sandbox/<name>.sql` | The SQL the sandbox actually ran |

Supported in v1: select, join (left / inner), filters, group by (with day / week / month / quarter / year), sum / average / count / count unique / min / max, masking (hash / redact), table or view, schedule. Window functions, incremental merges and streaming are out of scope for v1.

---

## API

| Capability | Endpoint |
|---|---|
| Platforms and how each names tables | `GET /api/platforms` |
| Catalog (all, or keyword search `?q=`) | `GET /api/catalog` |
| Dataset detail: columns, tags, PII-safe sample, lineage | `GET /api/catalog/{id}` |
| Lineage (upstream / downstream, column maps) | `GET /api/lineage/{id}` |
| Run history | `GET /api/runs` |
| Starter examples | `GET /api/examples` |
| Check and compile a spec without saving | `POST /api/compile` |
| Create / read / change pipelines | `POST /api/pipelines`, `GET /api/pipelines[/{id}]`, `PUT /api/pipelines/{id}` |
| Test in the sandbox | `POST /api/pipelines/{id}/test` |
| Apply a suggested fix and re-test | `POST /api/pipelines/{id}/fix` |
| Request deployment (queues for a human) | `POST /api/pipelines/{id}/request-deploy` |
| Approve / reject a deployment | `POST /api/actions/{id}/decision` |
| Plain-English request (returns a **draft**) | `POST /api/agent/ask` |
| Review AI answers | `GET /api/reviews`, `POST /api/reviews/{id}/decision`, `GET /api/reviews/{id}/published` |
| Audit log, AI call log, scorecard, kill switch | `GET /api/audit`, `GET /api/ai-calls`, `GET /api/governance`, `POST /api/governance/kill-switch` |

Interactive docs at `/docs`.

---

## Connect a real platform

The shipped connectors are **simulated**: no credentials, no cloud cost. To connect a real workspace, subclass the connector and override `deploy()`:

```python
# app/connectors/snowflake_live.py  (sketch)
import snowflake.connector
from .snowflake import SnowflakeConnector

class LiveSnowflakeConnector(SnowflakeConnector):
    def deploy(self, *, pipeline_id, target, rows_written, files):
        sql = load_compiled_sql(files[0])               # the compiled, tested SQL from your CI build
        with snowflake.connector.connect(...) as con:   # credentials from env / secrets manager
            con.cursor().execute(sql)
        return super().deploy(pipeline_id=pipeline_id, target=target,
                              rows_written=rows_written, files=files) | {"simulated": False}
```

Register it in `app/connectors/__init__.py`. The spec, compiler, sandbox, approvals and lineage don't change.

---

## Run it

```bash
pip install -r requirements.txt
cp .env.example .env          # ANTHROPIC_API_KEY is optional
uvicorn app.main:app --reload
# open http://localhost:8000
```

Or with PostgreSQL: `docker compose up --build`.

**Render:** push to GitHub, then *New → Blueprint* and pick this repo. `render.yaml` creates one free Docker web service; set `ANTHROPIC_API_KEY` (and optionally `ADMIN_TOKEN`) in the dashboard. The sandbox reseeds the fictional data each time the free instance wakes.

**Tests:** `pip install -r requirements-dev.txt && python -m pytest -q`. The agent is tested with a scripted fake client, so CI needs no secrets. A manual workflow run can add a live Claude smoke test.

---

## The demo data

**Lakeside Outfitters**, a fictional outdoor-gear retailer, with data spread across platforms the way it usually is:

| Dataset | Platform | Notes |
|---|---|---|
| Orders | Snowflake | 400 orders, 6 pointing at a customer that doesn't exist |
| Customers | Snowflake | Full name and email tagged **PII**; 2 customers with no region |
| Products (raw) | Databricks | Contains an old duplicate price row for one product |
| Products (current) | Databricks | Certified, one row per product |
| Website & App Events | Microsoft Fabric | Last loaded 6 days before the as-of date |
| Support Tickets | Microsoft Fabric | Satisfaction scores, some blank |

No real people, companies or figures.
