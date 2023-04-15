# Databricks notebook source
# MAGIC %run ../tests/test_transactions

# COMMAND ----------

# MAGIC %md 
# MAGIC #### EXECUTE TESTS

# COMMAND ----------

suite = unittest.TestLoader().loadTestsFromTestCase(TestMakeTransactions)
unittest.TextTestRunner(verbosity=2).run(suite)
