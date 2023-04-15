# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install -r /Workspace/Repos/mkrupski@bi4all.pt/SwissRe_task/requirements.txt

# COMMAND ----------

y = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson() 
res = json.loads(y)
user_name = res['tags']['user']

# COMMAND ----------

!mkdir /dbfs/FileStore/mkrupski_coding_task/input_files/
!mkdir /dbfs/FileStore/mkrupski_coding_task/TRANSACTIONS/
!cp /Workspace/Repos/$user_name/SwissRe_task/input_files/input_claim.csv /dbfs/FileStore/mkrupski_coding_task/input_files/
!cp /Workspace/Repos/$user_name/SwissRe_task/input_files/input_contract.csv /dbfs/FileStore/mkrupski_coding_task/input_files/
!mkdir /dbfs/FileStore/mkrupski_coding_task/test_files/
!mkdir /dbfs/FileStore/mkrupski_coding_task/test_output/
!cp /Workspace/Repos/$user_name/SwissRe_task/input_files/input_claim.csv /dbfs/FileStore/mkrupski_coding_task/test_files/
!cp /Workspace/Repos/$user_name/SwissRe_task/input_files/input_contract.csv /dbfs/FileStore/mkrupski_coding_task/test_files/

# COMMAND ----------

input_path = "/FileStore/mkrupski_coding_task/input_files/"
files_input = dbutils.fs.ls(input_path)
file_input_names = [file.name for file in files_input]
if file_input_names != ['input_claim.csv', 'input_contract.csv']:
    raise Exception(f"Check files in location {input_path}")

# COMMAND ----------

test_path = "/FileStore/mkrupski_coding_task/test_files/"
file_test = dbutils.fs.ls(test_path)
file_test_names = [file.name for file in file_test]
if file_test_names != ['input_claim.csv', 'input_contract.csv']:
    raise Exception(f"Check files in location {input_path}")

# COMMAND ----------

print("Configuration sucessful!")
