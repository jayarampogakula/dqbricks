from pyspark.sql import SparkSession


def ensure_objects(spark: SparkSession, quarantine_table: str, metrics_table: str, views: dict):
# Create schemas if needed
def ensure_schema(ident: str):
parts = ident.split('.')
if len(parts) == 3:
cat, sch, _ = parts
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
elif len(parts) == 2:
sch, _ = parts
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sch}")
for t in [quarantine_table, metrics_table, *views.values()]:
ensure_schema(t)


# Quarantine table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quarantine_table} (
table_name STRING,
rule_name STRING,
rule_type STRING,
_dq_is_valid BOOLEAN,
_dq_checked_at TIMESTAMP
) USING delta
""")


# Metrics table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metrics_table} (
table_name STRING,
rule_name STRING,
rule_type STRING,
total LONG,
passed LONG,
failed LONG,
pass_ratio DOUBLE,
batch_id LONG,
logged_at TIMESTAMP
) USING delta
""")


# Views
rule_results = views.get('rule_results')
if rule_results:
spark.sql(f"""
CREATE OR REPLACE VIEW {rule_results} AS
SELECT logged_at, batch_id, table_name, rule_name, rule_type,
total, passed, failed, pass_ratio
FROM {metrics_table}
""")


table_health = views.get('table_health')
if table_health:
spark.sql(f"""
CREATE OR REPLACE VIEW {table_health} AS
SELECT table_name,
avg(pass_ratio) AS avg_pass_ratio,
sum(failed) AS failed_rows,
max(logged_at) AS last_run
FROM {metrics_table}
GROUP BY table_name
""
