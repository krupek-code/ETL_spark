# Databricks notebook source
import os
import unittest
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../transformations/create_transactions

# COMMAND ----------

class TestMakeTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(
        cls,
        contracts_path="/FileStore/tables/tests/input_contract.csv",
        claim_path="/FileStore/tables/tests/input_claim.csv",
        output_path="/FileStore/tables/tests/TRANSACTIONS",
    ):
        cls.contracts_path = contracts_path
        cls.claim_path = claim_path
        cls.output_path = output_path
        cls.api = "https://api.hashify.net/hash/md4/hex?value="
        cls.transactions = MakeTransactions(
            cls.contracts_path, cls.claim_path, cls.output_path
        )
        cls.test_contract_df = (
            spark.read.option("delimeter", ",")
            .option("quote", "|")
            .csv(cls.contracts_path, header=True, inferSchema=True)
        )
        cls.test_claim_df = spark.read.csv(
            cls.claim_path, header=True, inferSchema=True
        )

    def test_read_data(self):
        test_contract_df, test_claim_df = self.transactions.read_data()
        expected = [5, 7]
        result_contract_df = test_contract_df.count()
        self.assertEqual(
            test_contract_df.count(),
            expected[0],
            "The file contract has not been written correctly.",
        )
        result_claim_df = test_claim_df.count()
        self.assertEqual(
            test_claim_df.count(),
            expected[1],
            "The file claim has not been written correctly.",
        )

    def test_add_contract_source_system(self):
        df = self.transactions.add_contract_source_system(self.test_claim_df)
        result = list(
            map(
                lambda row: row[0],
                df.select("CONTRACT_SOURCE_SYSTEM").distinct().collect(),
            )
        )
        expected = ["Europe 3"]
        self.assertEqual(
            result, expected, "The CONTRACT_SOURCE_SYSTEM result are not as expected."
        )

    def test_add_contract_source_system_id(self):
        df = self.transactions.add_contract_source_system_id(
            self.test_claim_df, self.test_contract_df
        )
        result = list(
            map(lambda row: row[0], df.select("CONTRACT_SOURCE_SYSTEM_ID").collect())
        )
        expected = [13767503, 97563756, 408124123, 656948536, 656948536, 656948536]
        self.assertListEqual(
            sorted(result),
            expected,
            "The CONTRACT_SOURCE_SYSTEM_ID result are not as expected.",
        )

    def test_add_source_system_id(self):
        df = self.transactions.add_source_system_id(self.test_claim_df)
        result = list(map(lambda row: row[0], df.select("SOURCE_SYSTEM_ID").collect()))
        expected = [
            "12066501",
            "39904634",
            "68545123",
            "7065313",
            "895168",
            "962234",
            "9845163",
        ]
        self.assertListEqual(
            sorted(result), expected, "The SOURCE_SYSTEM_ID result are not as expected."
        )

    def test_add_transaction_type(self):
        result = self.transactions.add_transaction_type(self.test_claim_df).select(
            "CLAIM_TYPE", "TRANSACTION_TYPE"
        )
        expected = {2: "Corporate", 1: "Private", None: "Unknown"}
        self.assertTrue(
            all(
                row["TRANSACTION_TYPE"] == expected[2]
                for row in result.collect()
                if row["CLAIM_TYPE"] == 2
            ),
            "Bad mapping CLAIM_TYPE 2 with TRANSACTION_TYPE.",
        )
        self.assertTrue(
            all(
                row["TRANSACTION_TYPE"] == expected[1]
                for row in result.collect()
                if row["CLAIM_TYPE"] == 1
            ),
            "Bad mapping CLAIM_TYPE 1 with TRANSACTION_TYPE.",
        )
        self.assertTrue(
            all(
                row["TRANSACTION_TYPE"] == expected[None]
                for row in result.collect()
                if row["CLAIM_TYPE"] == None
            ),
            "Bad mapping CLAIM_TYPE with TRANSACTION_TYPE.",
        )
        self.assertFalse(
            result.filter(col("TRANSACTION_TYPE").isNull()).count(),
            "Column TRANSACTION_TYPE has missing date values.",
        )

    def test_add_transaction_direction(self):
        result = self.transactions.add_transaction_direction(self.test_claim_df).select(
            "CLAIM_ID", "TRANSACTION_DIRECTION"
        )
        expected = {"CL": "COINSURANCE", "RX": "REINSURANCE", "Other": ""}
        self.assertTrue(
            all(
                row["TRANSACTION_DIRECTION"] == expected["CL"]
                for row in result.collect()
                if row["CLAIM_ID"].startswith("CL")
            ),
            "Bad mapping CLAIM_ID 'CL' with TRANSACTION_DIRECTION.",
        )
        self.assertTrue(
            all(
                row["TRANSACTION_DIRECTION"] == expected["RX"]
                for row in result.collect()
                if row["CLAIM_ID"].startswith("RL")
            ),
            "Bad mapping CLAIM_ID 'RL' with TRANSACTION_DIRECTION.",
        )
        self.assertTrue(
            all(
                row["TRANSACTION_DIRECTION"] == expected["Other"]
                for row in result.collect()
                if (
                    row["CLAIM_ID"].startswith("CL")
                    and row["CLAIM_ID"].startswith("RL")
                )
            ),
            "Bad mapping CLAIM_ID with TRANSACTION_DIRECTION.",
        )

    def test_dates(self):
        result_bussines_date = self.transactions.add_business_date(self.test_claim_df)
        self.assertFalse(
            result_bussines_date.filter(col("CREATION_DATE").isNull()).count(),
            "Column CREATION_DATE has missing date values.",
        )
        result_creation_date = self.transactions.add_creation_date(self.test_claim_df)
        self.assertFalse(
            result_creation_date.filter(col("CREATION_DATE").isNull()).count(),
            "Column CREATION_DATE has missing date values.",
        )
        result_system_timestamp = self.transactions.add_system_timestamp(
            self.test_claim_df
        )
        self.assertFalse(
            result_system_timestamp.filter(col("SYSTEM_TIMESTAMP").isNull()).count(),
            "Column SYSTEM_TIMESTAMP has missing date values.",
        )

    def test_add_nse_id(self):
        df = self.transactions.add_nse_id(self.test_claim_df, self.api)
        result = list(map(lambda row: row[0], df.select("NSE_ID").collect()))
        expected = [
            "01ffa3820d8f2d9421ca828a53be7449",
            "22e72db452e973fec96bd223cc67d213",
            "238cf61c3a7e1574bf12615d87697e45",
            "3be76cd7f9646dd9d79132c46b3dd603",
            "572deffbf618d7c3b5f3448eb9c5c4c1",
            "738f8bad9d69baf14f9264f8be6d821d",
            "96038ac921f5c5cc6405001d96760847",
        ]
        self.assertEqual(
            sorted(result),
            expected,
            "The NSE_ID result are not as expected. Check API.",
        )

    def test_output(self):
        result_output = self.transactions.transform_data()
        expected_schema = StructType(
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
        self.assertEqual(
            result_output.schema,
            expected_schema,
            "The schema of transactions_df does not match the expected output schema.",
        )

    def test_write_output(self):
        self.transactions.write_output(output_path=self.output_path)
        result = ["CONTRACT_SOURCE_SYSTEM=Europe 3", "_delta_log"]
        self.assertEqual(
            result[1],
            "_delta_log",
            "The format is not as expected. Expected is delta format.",
        )
        self.assertEqual(
            result[0],
            "CONTRACT_SOURCE_SYSTEM=Europe 3",
            "The partition is not as expected. Expected is CONTRACT_SOURCE_SYSTEM=Europe 3.",
        )

# COMMAND ----------

suite = unittest.TestLoader().loadTestsFromTestCase(TestMakeTransactions)
unittest.TextTestRunner(verbosity=2).run(suite)
