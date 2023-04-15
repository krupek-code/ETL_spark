# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

contract_schema = StructType(
    [
        StructField("SOURCE_SYSTEM", StringType(), False, {"primary_key": True}),
        StructField("CONTRACT_ID", LongType(), False, {"primary_key": True}),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True),
    ]
)

# COMMAND ----------

claim_schema = StructType(
    [
        StructField("SOURCE_SYSTEM", StringType(), False, {"primary_key": True}),
        StructField("CLAIM_ID", StringType(), False, {"primary_key": True}),
        StructField(
            "CONTRACT_SOURCE_SYSTEM", StringType(), False, {"primary_key": True}
        ),
        StructField("CONTRAT_ID", LongType(), False, {"primary_key": True}),
        StructField("CLAIM_TYPE", IntegerType(), True),
        StructField("DATE_OF_LOSS", StringType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("CREATION_DATE", StringType(), True),
    ]
)

# COMMAND ----------

output_schema = StructType(
    [
        StructField(
            "CONTRACT_SOURCE_SYSTEM", StringType(), True, {"primary_key": True}
        ),
        StructField(
            "CONTRACT_SOURCE_SYSTEM_ID", LongType(), True, {"primary_key": True}
        ),
        StructField("SOURCE_SYSTEM_ID", IntegerType(), True),
        StructField("TRANSACTION_TYPE", StringType(), False),
        StructField("TRANSACTION_DIRECTION", StringType(), True),
        StructField("CONFORMED_VALUE", DecimalType(16, 5), True),
        StructField("BUSINESS_DATE", DateType(), True),
        StructField("CREATION_DATE", TimestampType(), True),
        StructField("SYSTEM_TIMESTAMP", TimestampType(), True),
        StructField("NSE_ID", StringType(), False, {"primary_key": True}),
    ]
)
