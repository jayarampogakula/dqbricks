# DQBricks v3 — Databricks Delta Data Quality Framework

DQBricks v3 is a **turn‑key, pluggable data quality framework** for Databricks and Delta Lake.
It requires only a single **config.yaml** file to define inputs and rules. Optionally, you can add custom rules in **custom_rules.py**.
The framework handles everything else:
- Discovers target tables (table / schema / catalog)
- Runs batch or streaming (Auto Loader, CDF incremental)
- Creates quarantine + metrics tables automatically
- Creates dashboard views automatically (ready for Databricks SQL)

---

## 🚀 Features
- **Scopes**: validate a single table, all tables in a schema, or a whole catalog.
- **Rules**: built-in checks (`not_null`, `check`, `range`, `in_set`, `freshness`, `uniqueness`), plus custom rules via plugin.
- **Batch + Streaming**: works on static Delta tables or Auto Loader streams.
- **Incremental**: optional Change Data Feed (CDF) support for deltas only.
- **Dashboards**: auto-creates views for rule results, table health, and column nulls.
- **Turn‑key**: one config file drives the entire process.

---

## 📁 Project Structure
```
 dqbricks_v3/
  ├─ config.yaml              # Main config (edit this)
  ├─ custom_rules.py          # Optional custom rules
  ├─ run_notebook.py          # Runner (Databricks notebook/job)
  ├─ dqcore/
  │   ├─ engine.py            # Core engine (batch + streaming)
  │   ├─ registry.py          # Rule registry
  │   ├─ targets.py           # Target discovery
  │   └─ bootstrap.py         # Auto-create tables + views
  └─ rules/
      ├─ builtin.py           # Built-in rules
      └─ base.py              # Rule base class
```

---

## ⚡ Quick Start

### 1) Install
- Clone/copy this repo into a Databricks **Repo** or Workspace folder.
- Install PyYAML if not already:
  ```
  %pip install pyyaml
  ```

### 2) Configure
Edit **config.yaml**:
```yaml
quarantine_table: dq.quarantine
metrics_table: dq.metrics
views:
  rule_results: dq.v_dq_rule_results
  table_health: dq.v_dq_table_health
  column_nulls: dq.v_dq_column_nulls

mode: batch    # or streaming
scope: schema
catalog: hive_metastore
schema: sales
include: ["*"]
exclude: ["_dq_*", "tmp*"]

rules:
  - type: not_null
    name: nn_id
    cols: [id]
  - type: range
    name: amt_non_negative
    col: amount
    min: 0
```

### 3) Run
In a notebook:
```python
%run /Workspace/Repos/<you>/dqbricks_v3/run_notebook.py
```

Or as a Databricks Job (spark-python task):
```bash
spark-submit run_notebook.py /Workspace/Repos/<you>/dqbricks_v3/config.yaml
```

---

## 🖥️ Dashboards
The runner auto-creates the following views:
- **dq.v_dq_rule_results** → per rule, pass/fail counts, ratio
- **dq.v_dq_table_health** → overall health per table
- **dq.v_dq_column_nulls** → column-level null ratios (for `not_null` rules)

Import `DQBricks v3 — Databricks SQL Notebook (DQ Dashboards).sql` to get prebuilt queries:
- KPI tiles (overall pass ratio, failed rows, tables monitored)
- Pass ratio trends
- Top failing rules
- Table health snapshot
- Column null heatmap
- Quarantine drilldowns

---

## 🔧 Custom Rules
Define your own rule in `custom_rules.py`:
```python
from rules.base import Rule
from dqcore.registry import RuleRegistry
from pyspark.sql.functions import col, length

class MaxLength(Rule):
    NAME = 'max_length'
    def build(self):
        return length(col(self.params['col'])) <= int(self.params['max'])

RuleRegistry.register(MaxLength.NAME, MaxLength)
```
Reference in YAML:
```yaml
rules:
  - type: max_length
    col: description
    max: 200
```

---

## 📜 License
This project is licensed under the **Apache 2.0 License**.

```
Copyright 2025 <Your Name/Org>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

## 🙌 Credits
Built for Databricks Delta Lake users who need **plug‑and‑play data quality checks** without writing boilerplate pipelines.
