# Databricks notebook source
# MAGIC %md
# MAGIC #### Configure directory

# COMMAND ----------

import sys
import os
import json

# COMMAND ----------

sys.path.append(os.path.abspath(f'/Workspace/Repos/{user_name}/SwissRe_task/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run Databricks configuration file

# COMMAND ----------

# MAGIC %run ../configs/databricks_files_config
