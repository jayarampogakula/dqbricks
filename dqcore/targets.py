from typing import List, Tuple
from pyspark.sql import SparkSession
from fnmatch import fnmatch


def list_tables(spark: SparkSession, catalog: str = None, schema: str = None, include: List[str] = None, exclude: List[str] = None) -> List[Tuple[str,str,str]]:
include = include or ["*"]
exclude = exclude or []
rows = []
if catalog and schema:
rows = [(catalog, schema, t.name) for t in spark.catalog.listTables(f"{catalog}.{schema}") if t.tableType != 'TEMP']
elif catalog:
for db in spark.catalog.listDatabases(catalog):
rows += [(catalog, db.name, t.name) for t in spark.catalog.listTables(f"{catalog}.{db.name}") if t.tableType != 'TEMP']
elif schema:
current_catalog = spark.sql("SELECT current_catalog()").first()[0]
rows = [(current_catalog, schema, t.name) for t in spark.catalog.listTables(schema) if t.tableType != 'TEMP']
else:
for cat in [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]:
for db in spark.catalog.listDatabases(cat):
rows += [(cat, db.name, t.name) for t in spark.catalog.listTables(f"{cat}.{db.name}") if t.tableType != 'TEMP']


def match(name: str, patterns: List[str]):
return any(fnmatch(name, p) for p in patterns)


return [r for r in rows if match(r[2], include) and not match(r[2], exclude)]
