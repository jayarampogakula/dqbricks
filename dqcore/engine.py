# dqcore/engine.py
from __future__ import annotations

from typing import Dict, List, Tuple, Optional
from functools import reduce
import rules.builtin
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as Fsum, count as Fcount
)

from dqcore.registry import RuleRegistry
import importlib; importlib.import_module("custom_rules")
print("Registered:", sorted(RuleRegistry.names()))

class DQEngine:
    """
    Core engine for DQBricks v3.

    - compile_rules:   config -> [(rule_name, rule_type, rule_obj)]
    - evaluate:        single-pass predicate evaluation -> (good_df, bad_df, metrics_df)
    - write:           append quarantine + metrics to Delta tables
    - enforce_batch:   run on a static DataFrame
    - enforce_stream:  foreachBatch runner for Structured Streaming / Auto Loader
    """

    def __init__(
        self,
        spark: SparkSession,
        quarantine_table: str,
        metrics_table: str,
        views: Optional[dict] = None,
    ):
        self.spark = spark
        self.quarantine_table = quarantine_table
        self.metrics_table = metrics_table
        self.views = views or {}

    # ---------- helpers ----------

    @staticmethod
    def _rule_name_list(rules: List[Tuple[str, str, object]]) -> List[str]:
        return [name for (name, _rtype, _rule) in rules]

    def compile_rules(self, rule_cfgs: List[Dict]) -> List[Tuple[str, str, object]]:
        """
        Turn config dicts into instantiated rule objects.
        Returns list of (rule_name, rule_type, rule_instance).
        """
        compiled: List[Tuple[str, str, object]] = []
        for i, cfg in enumerate(rule_cfgs):
            rtype = cfg.get("type")
            if not rtype:
                raise ValueError(f"Rule at index {i} missing 'type'")
            name = cfg.get("name") or f"rule_{i+1}_{rtype}"
            rule_cls = RuleRegistry.get(rtype)
            rule = rule_cls.from_config(cfg)
            compiled.append((name, rtype, rule))
        return compiled

    def evaluate(
        self,
        df: DataFrame,
        fq_table: str,
        rules: List[Tuple[str, str, object]],
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Evaluate all rules in a single pass and produce:
          - good: rows where ALL predicates are true
          - bad:  rows where ANY predicate is false (includes metadata cols)
          - metrics: one row per rule with totals & pass/fail counts and pass_ratio
        """
        if not rules:
            # Nothing to check: everything passes, empty metrics
            ts = current_timestamp()
            good = df.withColumn("_dq_checked_at", ts)
            bad = df.limit(0)
            metrics_df = self.spark.createDataFrame([], "table_name string, rule_name string, rule_type string, total long, passed long, failed long, pass_ratio double")
            return good, bad, metrics_df

        # 1) Single-pass: attach one boolean column per rule
        pred_cols = [r.build().alias(name) for (name, _rtype, r) in rules]
        dfp = df.select("*", *pred_cols)

        # 2) Split good/bad using conjunction of all predicates
        names = self._rule_name_list(rules)
        all_pass = reduce(lambda a, b: a & b, [col(n) for n in names])
        good = dfp.filter(all_pass).drop(*names)
        bad = dfp.filter(~all_pass)

        # 3) Metrics in one aggregation (no per-rule actions)
        agg_exprs = [Fsum(col(n).cast("int")).alias(n) for n in names]
        agg_exprs.append(Fcount(lit(1)).alias("_total"))
        agg_row: Row = dfp.agg(*agg_exprs).collect()[0]
        total = int(agg_row["_total"]) if "_total" in agg_row else 0

        metrics_rows = []
        for (name, rtype, _rule) in rules:
            passed = int(agg_row[name]) if total else 0
            failed = total - passed
            ratio = 0.0 if total == 0 else (passed / total)
            metrics_rows.append((
                fq_table, name, rtype, total, passed, failed, ratio
            ))

        metrics_df = self.spark.createDataFrame(
            metrics_rows,
            schema="""
                table_name string,
                rule_name  string,
                rule_type  string,
                total      long,
                passed     long,
                failed     long,
                pass_ratio double
            """,
        )

        # 4) Annotate rows with metadata for downstream use
        ts = current_timestamp()
        good = good.withColumn("_dq_checked_at", ts)

        # Attach table & generic flags for quarantine rows
        bad = (bad
               .withColumn("_dq_checked_at", ts)
               .withColumn("_dq_is_valid", lit(False))
               .withColumn("table_name", lit(fq_table)))
        # (Optional) you can enrich bad rows with the specific failing rule(s)
        # by exploding the failed predicates; keeping minimal here for scale.

        return good, bad, metrics_df

    def write(
        self,
        bad: DataFrame,
        metrics: DataFrame,
        batch_id: Optional[int] = None
    ) -> None:
        """Append quarantine + metrics to Delta tables."""
        if bad is not None:
            bad.write.mode("append").format("delta").saveAsTable(self.quarantine_table)

        if metrics is not None:
            out = metrics.withColumn("logged_at", current_timestamp())
            if batch_id is not None:
                out = out.withColumn("batch_id", lit(int(batch_id)))
            out.write.mode("append").format("delta").saveAsTable(self.metrics_table)

    # ---------- public APIs ----------

    def enforce_batch(
        self,
        df: DataFrame,
        fq_table: str,
        rule_cfgs: List[Dict]
    ) -> DataFrame:
        """Run DQ checks on a static DataFrame, write side effects, return only passing rows."""
        rules = self.compile_rules(rule_cfgs)
        good, bad, mdf = self.evaluate(df, fq_table, rules)
        self.write(bad, mdf)
        return good

    def monitor_batch(
        self,
        df: DataFrame,
        fq_table: str,
        rule_cfgs: List[Dict]
    ) -> DataFrame:
        """Compute metrics only (no quarantine), return original df."""
        rules = self.compile_rules(rule_cfgs)
        _good, _bad, mdf = self.evaluate(df, fq_table, rules)
        # write only metrics
        self.write(self.spark.createDataFrame([], _good.schema), mdf)  # empty quarantine write is skipped
        return df

    def enforce_stream(
        self,
        sdf: DataFrame,
        fq_out_table: str,
        rule_cfgs: List[Dict],
        checkpoint: str
    ):
        """
        Structured Streaming helper (use with Auto Loader source).
        Writes good rows to fq_out_table, quarantine/metrics to configured tables.
        """
        def _foreach_batch(batch_df: DataFrame, batch_id: int):
            rules = self.compile_rules(rule_cfgs)
            good, bad, mdf = self.evaluate(batch_df, fq_out_table, rules)
            self.write(bad, mdf, batch_id=batch_id)
            (good
             .withColumn("_dq_checked_at", current_timestamp())
             .write.mode("append").format("delta").saveAsTable(fq_out_table))

        return (
            sdf.writeStream
               .foreachBatch(_foreach_batch)
               .option("checkpointLocation", checkpoint)
        )
