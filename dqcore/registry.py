from typing import Dict, Type
from rules.base import Rule


class RuleRegistry:
    _rules: Dict[str, Type[Rule]] = {}


    @classmethod
    def register(cls, name: str, rule_cls: Type[Rule]):
        key = name.lower().strip()
        if key in cls._rules:
            raise ValueError(f"Rule '{name}' already registered")
        cls._rules[key] = rule_cls


    @classmethod
    def get(cls, name: str) -> Type[Rule]:
        if name.lower().strip() not in cls._rules:
            raise ValueError(f"Unknown rule type '{name}'. Registered: {list(cls._rules)})")
        return cls._rules[name.lower().strip()]


    @classmethod
    def names(cls):
        return sorted(cls._rules.keys())
