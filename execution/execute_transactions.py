# Databricks notebook source
# MAGIC %run ../transformations/create_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dispaly schema and output 

# COMMAND ----------

transaction = MakeTransactions()
transaction_df = transaction.transform_data()
transaction_df.printSchema()

# COMMAND ----------

transaction_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output

# COMMAND ----------

transaction.write_output()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read output

# COMMAND ----------

df_output = spark.read.format("delta").load(transaction.output_path)
df_output.display()
