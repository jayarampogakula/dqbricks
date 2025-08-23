# dqcore/targets.py
from __future__ import annotations
from typing import List, Tuple, Optional
from pyspark.sql import SparkSession
from fnmatch import fnmatch

# Target tuple: (catalog, schema, table)
Target = Tuple[Optional[str], str, str]

def _match(name: str, patterns: List[str]) -> bool:
    return any(fnmatch(name, p) for p in patterns) if patterns else True

def _is_real_table(t) -> bool:
    # Exclude temp/views by default; Databricks returns tableType among: MANAGED, EXTERNAL, VIEW, TEMP
    return (getattr(t, "tableType", "") or "").upper() not in {"TEMP", "VIEW"}

def _current_catalog(spark: SparkSession) -> Optional[str]:
    try:
        return spark.sql("SELECT current_catalog()").first()[0]
    except Exception:
        # On workspaces without Unity Catalog this function may not exist
        return None

def list_tables(
    spark: SparkSession,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> List[Target]:
    """
    Discover Delta/SQL tables across a table, schema, or catalog scope.

    Args:
        catalog: target catalog (None means use current catalog for schema-level, or enumerate all)
        schema: target schema (database)
        include: glob patterns of table names to include (default ['*'])
        exclude: glob patterns of table names to exclude

    Returns:
        List of (catalog, schema, table) tuples. catalog may be None on HMS-only workspaces.
    """
    include = include or ["*"]
    exclude = exclude or []
    rows: List[Target] = []

    # Helper to add tables for a given (catalog, schema)
    def add_schema_tables(cat: Optional[str], sch: str):
        full_db = f"{cat}.{sch}" if cat else sch
        for t in spark.catalog.listTables(full_db):
            if not _is_real_table(t):
                continue
            if not _match(t.name, include) or _match(t.name, exclude):
                continue
            rows.append((cat, sch, t.name))

    if schema and catalog:
        add_schema_tables(catalog, schema)

    elif schema and not catalog:
        # Use current catalog if present (Unity Catalog), else None (Hive Metastore-only)
        cat = _current_catalog(spark)
        add_schema_tables(cat, schema)

    elif catalog and not schema:
        # All schemas in a given catalog
        # Databricks: SHOW SCHEMAS IN <catalog>
        result = spark.sql(f"SHOW SCHEMAS IN {quote_ident(catalog)}").collect()
        for r in result:
            sch = r["databaseName"] if "databaseName" in r.asDict() else r[0]
            add_schema_tables(catalog, sch)

    else:
        # No catalog or schema provided → enumerate all catalogs (if supported), else all databases in HMS
        try:
            cats = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
            for cat in cats:
                dbs = spark.sql(f"SHOW SCHEMAS IN {quote_ident(cat)}").collect()
                for r in dbs:
                    sch = r["databaseName"] if "databaseName" in r.asDict() else r[0]
                    add_schema_tables(cat, sch)
        except Exception:
            # Fallback: Hive Metastore only
            for db in spark.catalog.listDatabases():
                add_schema_tables(None, db.name)

    return rows

def fq_name(cat: Optional[str], sch: str, tbl: str) -> str:
    """Return fully qualified name with backticks where needed."""
    if cat:
        return f"{quote_ident(cat)}.{quote_ident(sch)}.{quote_ident(tbl)}"
    return f"{quote_ident(sch)}.{quote_ident(tbl)}"

def quote_ident(ident: str) -> str:
    """Quote a single identifier component with backticks (no dots allowed)."""
    ident = ident.strip("`")
    if "." in ident:
        raise ValueError(f"Identifier component contains dot: {ident}")
    return f"`{ident}`"
