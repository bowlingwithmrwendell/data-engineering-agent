"""The compiler: one spec, correct code for every platform, byte-for-byte reproducible."""
import copy

from app import compiler, information
from app.seed import EXAMPLES
from app.spec import PipelineSpec, resolve, summarize

REVENUE = EXAMPLES[0]["spec"]


def _resolved(session, spec=REVENUE, platform=None):
    d = copy.deepcopy(spec)
    if platform:
        d["platform"] = platform
    cat = information.catalog_map(session)
    r, issues = resolve(PipelineSpec.model_validate(d), cat)
    return r, issues, cat


def test_same_spec_same_code(session):
    r, _, cat = _resolved(session)
    assert compiler.artifacts(r, cat) == compiler.artifacts(r, cat)


def test_platform_naming_and_ddl(session):
    for platform, needle in [("databricks", "CREATE OR REPLACE TABLE lakeside.gold.monthly_revenue_by_region"),
                             ("snowflake", "CREATE OR REPLACE TABLE LAKESIDE.GOLD.MONTHLY_REVENUE_BY_REGION"),
                             ("fabric", "CREATE TABLE lakeside_wh.gold.monthly_revenue_by_region")]:
        r, _, cat = _resolved(session, platform=platform)
        sql = compiler.platform_sql(r, cat)
        assert needle in sql, platform


def test_date_truncation_per_dialect(session):
    r, _, cat = _resolved(session)
    assert "DATE_TRUNC('MONTH', orders.order_date)" in compiler.platform_sql(r, cat, "snowflake")
    assert "date_trunc('MONTH', orders.order_date)" in compiler.platform_sql(r, cat, "databricks")
    assert "DATETRUNC(month, orders.order_date)" in compiler.platform_sql(r, cat, "fabric")
    assert "date(orders.order_date, 'start of month')" in compiler.sandbox_sql(r)
    model, _ = compiler.dbt(r, cat)
    assert "dbt.date_trunc('month', 'orders.order_date')" in model


def test_group_by_repeats_expressions_for_tsql(session):
    r, _, cat = _resolved(session, platform="fabric")
    sql = compiler.platform_sql(r, cat)
    group = sql.split("GROUP BY", 1)[1]
    assert "CAST(DATETRUNC(month, orders.order_date) AS DATE)" in group   # T-SQL can't group by alias


def test_pyspark_only_where_supported(session):
    r, _, cat = _resolved(session, platform="snowflake")
    assert compiler.pyspark(r, cat) is None
    r, _, cat = _resolved(session, platform="databricks")
    py = compiler.pyspark(r, cat)
    assert 'F.date_trunc("month", F.col("orders.order_date")).cast("date").alias("order_date_month")' in py
    assert 'saveAsTable("lakeside.gold.monthly_revenue_by_region")' in py
    r, _, cat = _resolved(session, platform="fabric")
    assert 'spark.table("lakeside_lh.raw.orders")' in compiler.pyspark(r, cat)


def test_string_literals_are_escaped(session):
    d = copy.deepcopy(REVENUE)
    d["filters"] = [{"column": "orders.status", "op": "=", "value": "x' OR '1'='1"}]
    r, _, cat = _resolved(session, d)
    sql = compiler.sandbox_sql(r)
    assert "'x'' OR ''1''=''1'" in sql


def test_masking_per_dialect(session):
    spec = copy.deepcopy(EXAMPLES[2]["spec"])
    spec["masks"] = [{"column": "customers.email", "method": "hash"},
                     {"column": "customers.full_name", "method": "redact"}]
    r, issues, cat = _resolved(session, spec)
    assert not [i for i in issues if i.level == "error"]
    assert "SHA2(customers.email, 256)" in compiler.platform_sql(r, cat, "snowflake")
    assert "sha2(CAST(customers.email AS STRING), 256)" in compiler.platform_sql(r, cat, "databricks")
    assert "HASHBYTES('SHA2_256'" in compiler.platform_sql(r, cat, "fabric")
    assert "ELSE '***' END AS full_name" in compiler.sandbox_sql(r)


def test_dbt_tests_follow_the_grain(session):
    r, _, cat = _resolved(session)
    _, schema = compiler.dbt(r, cat)
    assert "unique_combination_of_columns" in schema
    assert "combination_of_columns: [region, order_date_month]" in schema


def test_schedule_artifacts(session):
    for platform, path, needle in [("databricks", "deploy/databricks.yml", "quartz_cron_expression"),
                                   ("snowflake", "deploy/task.sql", "SCHEDULE = 'USING CRON 0 6 * * * UTC'"),
                                   ("fabric", "deploy/pipeline.json", '"expression": "0 6 * * *"')]:
        r, _, cat = _resolved(session, platform=platform)
        files = {f["path"]: f for f in compiler.artifacts(r, cat)}
        assert needle in files[path]["content"], platform


def test_plain_english_summary(session):
    cat = information.catalog_map(session)
    text = summarize(PipelineSpec.model_validate(REVENUE), cat)
    assert text.startswith("Revenue (total amount)")
    assert "Orders joined to Customers" in text and "Snowflake" in text
