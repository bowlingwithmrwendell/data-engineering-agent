from __future__ import annotations

import json

from .base import CRON, PlatformConnector


class FabricConnector(PlatformConnector):
    key = "fabric"
    label = "Microsoft Fabric"
    sql_dialect = "Fabric Warehouse T-SQL"
    supports_pyspark = True
    blurb = ("Unified analytics on OneLake. SQL runs in a Fabric Warehouse (T-SQL, named "
             "warehouse.schema.table); PySpark runs in a Lakehouse notebook; pipelines are scheduled "
             "in Data Factory.")
    deploy_verb = "CREATE TABLE AS SELECT"

    def table_name(self, schema: str, table: str) -> str:
        return f"{self.catalog_name}_wh.{schema}.{table}"

    def schedule_artifact(self, pipeline_name, target, sql, schedule):
        name = target.split(".")[-1]
        pipeline = {
            "name": f"pl_{name}",
            "properties": {
                "description": f"Data Engineering Agent pipeline: {pipeline_name}",
                "activities": [{
                    "name": f"Build {name}",
                    "type": "Script",
                    "typeProperties": {"scripts": [{"type": "NonQuery", "text": sql.strip()}]},
                    "linkedService": {"name": f"{self.catalog_name}_wh", "type": "DataWarehouse"},
                }],
            },
        }
        if schedule != "manual":
            pipeline["schedule"] = {"type": "Cron", "expression": CRON[schedule], "timeZone": "UTC"}
        return "pipeline.json", "json", json.dumps(pipeline, indent=2) + "\n"
