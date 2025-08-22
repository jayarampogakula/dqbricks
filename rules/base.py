from typing import Any, Dict
from pyspark.sql.column import Column


class Rule:
NAME = "base"
def __init__(self, **params):
self.params = params
@classmethod
def from_config(cls, cfg: Dict[str, Any]):
params = {k: v for k, v in cfg.items() if k not in {"type", "name"}}
return cls(**params)
def build(self) -> Column:
raise NotImplementedError
def describe(self) -> Dict[str, Any]:
return {"type": self.NAME, **self.params}
