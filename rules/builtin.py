# rules/builtin.py
from __future__ import annotations
from typing import List, Any
from pyspark.sql.functions import (
    col, expr, lit, to_timestamp, current_timestamp,
    row_number
)
from pyspark.sql.window import Window
from rules.base import Rule
from dqcore.registry import RuleRegistry

# -----------------------------
# Built-in rule implementations
# -----------------------------

class NotNull(Rule):
    NAME = "not_null"
    def build(self):
        cols: List[str] = self.params.get("cols", [])
        if not cols:
            # If no columns provided, treat as a no-op that always passes
            return lit(True)
        cond = None
        for c in cols:
            piece = col(c).isNotNull()
            cond = piece if cond is None else (cond & piece)
        return cond

class Check(Rule):
    NAME = "check"
    def build(self):
        e = self.params.get("expr")
        if not e:
            raise ValueError("check rule requires 'expr'")
        # e should be a Spark SQL boolean expression
        return expr(e)

class Range(Rule):
    NAME = "range"
    def build(self):
        c = self.params.get("col")
        lo = self.params.get("min", None)
        hi = self.params.get("max", None)
        if not c:
            raise ValueError("range rule requires 'col'")
        cond = lit(True)
        if lo is not None:
            cond = cond & (col(c) >= lit(lo))
        if hi is not None:
            cond = cond & (col(c) <= lit(hi))
        return cond

class InSet(Rule):
    NAME = "in_set"
    def build(self):
        c = self.params.get("col")
        values = self.params.get("values", [])
        if not c or not isinstance(values, list) or len(values) == 0:
            raise ValueError("in_set requires 'col' and a non-empty 'values' list")
        # For large dictionaries, consider broadcasting a lookup table externally.
        return col(c).isin(values)

class Freshness(Rule):
    NAME = "freshness"
    def build(self):
        ts_col = self.params.get("ts_col")
        max_lag_min = int(self.params.get("max_lag_minutes", 60))
        if not ts_col:
            raise ValueError("freshness requires 'ts_col'")
        ts = to_timestamp(col(ts_col))
        # current_timestamp - ts <= max_lag_min
        return (current_timestamp().cast("long") - ts.cast("long") <= max_lag_min * 60)

class Uniqueness(Rule):
    NAME = "uniqueness"
    def build(self):
        cols: List[str] = self.params.get("cols", [])
        if not cols:
            raise ValueError("uniqueness requires 'cols'")
        # Create a deterministic window over the key columns. We only allow the first row per key.
        # Note: row_number requires an orderBy; using all key columns ensures deterministic tie-break with values.
        # For massive datasets, consider de-duping upstream and using an anti-join for failures in a custom rule.
        w = Window.partitionBy(*[col(c) for c in cols]).orderBy(*[col(c) for c in cols])
        return (row_number().over(w) == 1)

# -----------------------------
# Registration
# -----------------------------
RuleRegistry.register(NotNull.NAME, NotNull)
RuleRegistry.register(Check.NAME, Check)
RuleRegistry.register(Range.NAME, Range)
RuleRegistry.register(InSet.NAME, InSet)
RuleRegistry.register(Freshness.NAME, Freshness)
RuleRegistry.register(Uniqueness.NAME, Uniqueness)
