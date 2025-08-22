# How you run it in Databricks

  1. Put the dqbricks_v3/ folder into a Repo or Workspace path.
  
  2. In a notebook on your cluster:
     
      %pip install pyyaml
      import sys
      sys.path.append('/Workspace/Repos/<you>/dqbricks_v3')

  3. Edit config.yaml for your environment (paths, scope, rules).
  4. Run:
     
     %run /Workspace/Repos/<you>/dqbricks_v3/run_notebook.py
     
     Or as a Job task with spark-python and pass the config path as the first arg.
## What you get automatically
  Delta tables: dq.quarantine, dq.metrics (names from your config)
  
  Views for dashboards:
  
      dq.v_dq_rule_results – per rule/pass ratio over time
      
      dq.v_dq_table_health – snapshot by table
      
      dq.v_dq_column_nulls – null% per column (when you name not-null rules like nn_<col>)

## Open Databricks SQL and build charts directly on those views:

    Line chart of pass_ratio by time (from v_dq_rule_results)
    
    Bar chart of top failing rules (sum failed, last 24h)
    
    Table of table health (avg pass ratio, failed rows, last run)
