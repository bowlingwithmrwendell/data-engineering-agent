from __future__ import annotations

from .base import CRON, PlatformConnector


class SnowflakeConnector(PlatformConnector):
    key = "snowflake"
    label = "Snowflake"
    sql_dialect = "Snowflake SQL"
    supports_pyspark = False
    blurb = ("Cloud data warehouse. Tables are named DATABASE.SCHEMA.TABLE; pipelines run as "
             "Snowflake Tasks on a virtual warehouse.")
    deploy_verb = "CREATE OR REPLACE"

    def table_name(self, schema: str, table: str) -> str:
        return f"{self.catalog_name.upper()}.{schema.upper()}.{table.upper()}"

    def schedule_artifact(self, pipeline_name, target, sql, schedule):
        task = "BUILD_" + target.split(".")[-1]
        sched = ("" if schedule == "manual" else
                 f"  SCHEDULE = 'USING CRON {CRON[schedule]} UTC'\n")
        body = sql.rstrip().rstrip(";")
        content = (f"-- Snowflake Task for: {pipeline_name}\n"
                   f"CREATE OR REPLACE TASK {self.catalog_name.upper()}.GOLD.{task}\n"
                   "  WAREHOUSE = TRANSFORM_WH\n"
                   f"{sched}"
                   "AS\n" + "\n".join("  " + ln for ln in body.splitlines()) + ";\n\n"
                   + ("" if schedule == "manual" else
                      f"ALTER TASK {self.catalog_name.upper()}.GOLD.{task} RESUME;\n"))
        return "task.sql", "sql", content
