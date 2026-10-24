from __future__ import annotations

from .base import CRON, PlatformConnector


class DatabricksConnector(PlatformConnector):
    key = "databricks"
    label = "Databricks"
    sql_dialect = "Databricks SQL (Spark SQL) on Unity Catalog"
    supports_pyspark = True
    blurb = ("Lakehouse on Delta Lake. Tables are named catalog.schema.table in Unity Catalog; "
             "pipelines run as Databricks Jobs defined in an Asset Bundle.")
    deploy_verb = "CREATE OR REPLACE"

    def table_name(self, schema: str, table: str) -> str:
        return f"{self.catalog_name}.{schema}.{table}"

    def schedule_artifact(self, pipeline_name, target, sql, schedule):
        job = target.split(".")[-1]
        trig = ("" if schedule == "manual" else
                f"      schedule:\n        quartz_cron_expression: \"{_quartz(CRON[schedule])}\"\n"
                f"        timezone_id: UTC\n")
        content = (f"# Databricks Asset Bundle job for: {pipeline_name}\n"
                   "resources:\n  jobs:\n"
                   f"    {job}:\n      name: \"{pipeline_name}\"\n{trig}"
                   "      tasks:\n"
                   f"        - task_key: build_{job}\n"
                   "          sql_task:\n"
                   "            warehouse_id: ${var.warehouse_id}\n"
                   f"            file:\n              path: ./sql/{job}.sql\n")
        return f"databricks.yml", "yaml", content


def _quartz(cron5: str) -> str:
    m, h, dom, mon, dow = cron5.split()
    if dow != "*":
        return f"0 {m} {h} ? * MON"
    return f"0 {m} {h} * * ?"
