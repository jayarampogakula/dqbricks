from typing import List
NAME = "range"
def build(self):
c = self.params.get("col")
lo = self.params.get("min")
hi = self.params.get("max")
if c is None:
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
if not c or not values:
raise ValueError("in_set requires 'col' and non-empty 'values'")
return col(c).isin(values)


class Freshness(Rule):
NAME = "freshness"
def build(self):
ts_col = self.params.get("ts_col")
max_lag_min = int(self.params.get("max_lag_minutes", 60))
if not ts_col:
raise ValueError("freshness requires 'ts_col'")
ts = to_timestamp(col(ts_col))
return (current_timestamp().cast("long") - ts.cast("long") <= max_lag_min * 60)


class Uniqueness(Rule):
NAME = "uniqueness"
def build(self):
cols = self.params.get("cols", [])
if not cols:
raise ValueError("uniqueness requires 'cols'")
w = Window.partitionBy(*[col(c) for c in cols]).orderBy(col(cols[0]))
return (row_number().over(w) == 1)


RuleRegistry.register(NotNull.NAME, NotNull)
RuleRegistry.register(Check.NAME, Check)
RuleRegistry.register(Range.NAME, Range)
RuleRegistry.register(InSet.NAME, InSet)
RuleRegistry.register(Freshness.NAME, Freshness)
RuleRegistry.register(Uniqueness.NAME, Uniqueness)
