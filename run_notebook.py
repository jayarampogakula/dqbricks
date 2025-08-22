# run_notebook.py — DQBricks v3 entrypoint (Databricks notebook or Job)

import sys, importlib, yaml
from pyspark.sql import SparkSession
from dqcore.bootstrap import ensure_objects
from dqcore.engine import DQEngine
from dqcore.targets import list_tables
import rules  # registers built-in rules

# Allow CLI arg: "python run_notebook.py /Workspace/.../config.yaml"
cfg_path = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

with open(cfg_path) as f:
    cfg = yaml.safe_load(f)

# Optional custom rules
if cfg.get('custom_rules_module'):
    importlib.import_module(cfg['custom_rules_module'])

# Ensure quarantine/metrics tables + dashboard views exist
ensure_objects(spark, cfg['quarantine_table'], cfg['metrics_table'], cfg.get('views', {}))

engine = DQEngine(spark, cfg['quarantine_table'], cfg['metrics_table'], cfg.get('views', {}))

# Resolve target tables
scope = cfg.get('scope', 'table')
include = cfg.get('include', ['*'])
exclude = cfg.get('exclude', [])

if scope == 'table':
    fq = f"{cfg.get('catalog')+'.' if cfg.get('catalog') else ''}{cfg['schema']}.{cfg['table']}"
    targets = [tuple(fq.split('.', 2))] if '.' in fq else [(None, cfg['schema'], cfg['table'])]
elif scope == 'schema':
    targets = list_tables(spark, catalog=cfg.get('catalog'), schema=cfg['schema'], include=include, exclude=exclude)
else:
    targets = list_tables(spark, catalog=cfg.get('catalog'), include=include, exclude=exclude)

# Execution mode
target_mode = cfg.get('mode', 'batch')

if target_mode == 'streaming':
    # Auto Loader streaming source
    src = (spark.readStream.format('cloudFiles')
           .option('cloudFiles.format', cfg['streaming']['input_format'])
           .option('cloudFiles.schemaLocation', cfg['streaming']['schema_location'])
           .option('rescuedDataColumn', '_rescued_data')
           .load(cfg['streaming']['input_path']))

    q = (engine
         .enforce_stream(src,
                         fq_out_table=cfg['streaming']['output_table'],
                         rule_cfgs=cfg['rules'],
                         checkpoint=cfg['streaming']['checkpoint'])
         .trigger(cfg['streaming']['trigger'])
         .start())
    q.awaitTermination()

else:
    for (cat, sch, tbl) in targets:
        fq_table = f"{cat}.{sch}.{tbl}" if cat else f"{sch}.{tbl}"
        df = spark.table(fq_table)

        # Choose rules: per-table override or defaults
        rules_cfg = cfg.get('tables', {}).get(fq_table, {}).get('rules', cfg['rules'])

        # Optional incremental via Delta Change Data Feed
        inc = cfg.get('incremental', {})
        if inc.get('use_cdf', False):
            spark.sql(f"ALTER TABLE {fq_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            df = (spark.read.format('delta')
                  .option('readChangeFeed', 'true')
                  .option('startingVersion', inc.get('starting_version', 0))
                  .table(fq_table)
                  .filter("_change_type IN ('insert','update_postimage')"))

        _ = engine.enforce_batch(df, fq_table, rules_cfg)
        print(f"Processed {fq_table} with {len(rules_cfg)} rules")

print("DQBricks v3 run complete.")
