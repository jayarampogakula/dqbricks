# dqcore/bootstrap.py
from pyspark.sql import SparkSession

def ensure_objects(
    spark: SparkSession,
    quarantine_table: str,
    metrics_table: str,
    views: dict
) -> None:
    """
    Create (if not exists):
      - Delta tables: quarantine, metrics
      - SQL views: rule_results, table_health, column_nulls (optional)
    Identifiers can be catalog.schema.table or schema.table.
    """

    def q_ident(ident: str) -> str:
        """Quote a dotted identifier with backticks: cat.sch.tbl -> `cat`.`sch`.`tbl`."""
        parts = [p.strip("`") for p in ident.split(".")]
        return ".".join(f"`{p}`" for p in parts)

    def ensure_schema(ident: str) -> None:
        """
        Ensure catalog/schema exist for a dotted identifier.
        - cat.sch.tbl  -> CREATE CATALOG/SCHEMA IF NOT EXISTS
        - sch.tbl      -> CREATE SCHEMA IF NOT EXISTS
        """
        parts = [p.strip("`") for p in ident.split(".")]
        if len(parts) == 3:
            cat, sch, _ = parts
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {q_ident(cat)}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q_ident(cat + '.' + sch)}")
        elif len(parts) == 2:
            sch, _ = parts
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q_ident(sch)}")
        else:
            # single-name is not expected here, but ignore gracefully
            pass

    # Ensure schemas for all target objects (tables + views)
    for t in [quarantine_table, metrics_table, *views.values()]:
        ensure_schema(t)

    # Quarantine table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {q_ident(quarantine_table)} (
            table_name     STRING,
            rule_name      STRING,
            rule_type      STRING,
            _dq_is_valid   BOOLEAN,
            _dq_checked_at TIMESTAMP
        ) USING delta
    """)

    # Metrics table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {q_ident(metrics_table)} (
            table_name STRING,
            rule_name  STRING,
            rule_type  STRING,
            total      LONG,
            passed     LONG,
            failed     LONG,
            pass_ratio DOUBLE,
            batch_id   LONG,
            logged_at  TIMESTAMP
        ) USING delta
    """)

    # Views (optional)
    rule_results = views.get("rule_results")
    if rule_results:
        spark.sql(f"""
            CREATE OR REPLACE VIEW {q_ident(rule_results)} AS
            SELECT
                logged_at,
                batch_id,
                table_name,
                rule_name,
                rule_type,
                total,
                passed,
                failed,
                pass_ratio
            FROM {q_ident(metrics_table)}
        """)

    table_health = views.get("table_health")
    if table_health:
        spark.sql(f"""
            CREATE OR REPLACE VIEW {q_ident(table_health)} AS
            SELECT
                table_name,
                AVG(pass_ratio) AS avg_pass_ratio,
                SUM(failed)     AS failed_rows,
                MAX(logged_at)  AS last_run
            FROM {q_ident(metrics_table)}
            GROUP BY table_name
        """)

    # Optional: column_nulls view (relies on 'not_null' rules named 'nn_<col>')
    column_nulls = views.get("column_nulls")
    if column_nulls:
        spark.sql(f"""
            CREATE OR REPLACE VIEW {q_ident(column_nulls)} AS
            SELECT
                table_name,
                regexp_extract(rule_name, 'nn_(.*)', 1) AS column_name,
                1 - pass_ratio AS null_ratio,
                logged_at
            FROM {q_ident(metrics_table)}
            WHERE rule_type = 'not_null'
        """)
