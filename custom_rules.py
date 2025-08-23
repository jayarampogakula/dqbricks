# custom_rules.py
from rules.base import Rule
from dqcore.registry import RuleRegistry
from pyspark.sql.functions import col, length


class MaxLength(Rule):
    NAME = 'max_length'
    def build(self):
        return length(col(self.params['col'])) <= int(self.params['max'])


# Register
RuleRegistry.register(MaxLength.NAME, MaxLength)
