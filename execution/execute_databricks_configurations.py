# Databricks notebook source
# MAGIC %md
# MAGIC #### Configure directory

# COMMAND ----------

import sys
import os
import json

# COMMAND ----------

y = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson() 
res = json.loads(y)
user_name = res['tags']['user']
sys.path.append(os.path.abspath(f'/Workspace/Repos/{user_name}/SwissRe_task/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Databricks configuration file

# COMMAND ----------

# MAGIC %run ../configs/databricks_files_config
