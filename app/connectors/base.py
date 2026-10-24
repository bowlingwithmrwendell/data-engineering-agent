"""Tool Calling Capabilities: the platform connector contract.

Every enterprise data platform plugs in through the same small interface, so
the rest of the agent never needs to know which platform it is talking to.

The shipped connectors are SIMULATED: they name tables the way the real
platform does, generate that platform's scheduling artifact, and "deploy" by
recording what would happen -- no credentials, no cloud cost. To connect a
real workspace, subclass the simulated connector and override `deploy()` (and
optionally `list_tables()`), then register it in connectors/__init__.py.
Nothing else in the codebase changes.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod


class PlatformConnector(ABC):
    key: str = ""             # databricks | snowflake | fabric
    label: str = ""           # display name
    sql_dialect: str = ""     # what the generated SQL targets
    supports_pyspark: bool = False
    catalog_name: str = "lakeside"
    blurb: str = ""

    # --- naming -------------------------------------------------------------------
    @abstractmethod
    def table_name(self, schema: str, table: str) -> str:
        """Fully-qualified name for schema.table on this platform."""

    def source_name(self, dataset: dict) -> str:
        """Where a catalog dataset is read from on this platform."""
        schema = "raw" if dataset["layer"] == "bronze" else dataset["layer"]
        return self.table_name(schema, dataset["id"])

    def target_name(self, layer: str, table: str) -> str:
        return self.table_name(layer, table)

    # --- orchestration artifacts -------------------------------------------------------
    @abstractmethod
    def schedule_artifact(self, pipeline_name: str, target: str, sql: str, schedule: str) -> tuple[str, str, str]:
        """(filename, language, content) for this platform's job/task definition."""

    # --- execution ------------------------------------------------------------------------
    def deploy(self, *, pipeline_id: int, target: str, rows_written: int, files: list[str]) -> dict:
        """Simulated deployment. Deterministic so the demo is reproducible."""
        job = f"{self.key}-job-{pipeline_id:04d}"
        digest = int(hashlib.sha256(f"{job}|{rows_written}".encode()).hexdigest(), 16)
        duration_ms = 1800 + rows_written * 3 + digest % 900
        log = [
            f"[{self.label}] connected (simulated workspace '{self.catalog_name}')",
            f"[git] branch de-agent/{target.split('.')[-1]} with {len(files)} file(s): {', '.join(files)}",
            "[ci] compile + sandbox checks re-run: passed",
            f"[{self.label}] {self.deploy_verb} {target}",
            f"[{self.label}] wrote {rows_written} row(s) in {duration_ms} ms",
            f"[{self.label}] schedule registered as {job}",
        ]
        return {"job_id": job, "status": "SUCCEEDED", "rows_written": rows_written,
                "duration_ms": duration_ms, "log": log, "simulated": True}

    deploy_verb = "created"

    def describe(self) -> dict:
        return {"key": self.key, "label": self.label, "sql_dialect": self.sql_dialect,
                "supports_pyspark": self.supports_pyspark, "mode": "simulated", "about": self.blurb,
                "naming_example": self.table_name("gold", "monthly_revenue")}


CRON = {"hourly": "0 * * * *", "daily": "0 6 * * *", "weekly": "0 6 * * 1"}
