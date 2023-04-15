# Databricks notebook source
from requests.exceptions import RequestException, ConnectionError
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit, regexp_replace, substring, to_date, to_timestamp, when
import requests

# COMMAND ----------

class MakeTransactions:
    """
    A class for transforming and writing data for a transactional system.
    Parameters:
    contracts_path (str): The path to the contracts CSV file.
    claim_path (str): The path to the claims CSV file.
    output_path (str): The path to write the output to.
    """

    def __init__(
        self,
        contracts_path="/FileStore/tables/swiss_re/input_contract.csv",
        claim_path="/FileStore/tables/swiss_re/input_claim.csv",
        output_path="/FileStore/tables/swiss_re/TRANSACTIONS/",
    ):
        self.contracts_path = contracts_path
        self.claim_path = claim_path
        self.output_path = output_path
        self.api = "https://api.hashify.net/hash/md4/hex?value="
        self.output_schema = StructType(
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

    def read_data(self) -> DataFrame:
        """
        Reads the contracts and claims data from their respective files.
        Returns:
            DataFrame: contract DataFrame
            DataFrame: claim DataFrame
        """
        contract_df = (
            spark.read.option("delimeter", ",")
            .option("quote", "|")
            .csv(self.contracts_path, header=True, inferSchema=True)
        )
        claim_df = spark.read.csv(self.claim_path, header=True, inferSchema=True)
        return contract_df, claim_df

    @staticmethod
    def add_contract_source_system(df: DataFrame) -> DataFrame:
        """
        Adds the 'CONTRACT_SOURCE_SYSTEM' columns to the DataFrame.
        'CONTRACT_SOURCE_SYSTEM' is a static string value 'Europe 3'.
        Args:
            df (DataFrame): The DataFrame to which the columns will be added.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3"))

    @staticmethod
    def add_contract_source_system_id(df: DataFrame, df_2: DataFrame) -> DataFrame:
        """
        Join contract and claim DataFrame to map Contract.CONTRACT_ID
        Args:
            df (DataFrame): claim DataFrame
            df_2 (DataFrame): contract DataFtame
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.join(
            df_2.select("CONTRACT_ID"),
            col("CONTRAT_ID") == col("CONTRACT_ID"),
            "leftsemi",
        ).withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("CONTRAT_ID"))

    @staticmethod
    def add_source_system_id(df: DataFrame) -> DataFrame:
        """
        Adds the 'SOURCE_SYSTEM_ID' column to the DataFrame.
        'SOURCE_SYSTEM_ID' is extracted from the 'CLAIM_ID' column.
        Args:
            df (DataFrame): The DataFrame to which the column will be added.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.withColumn(
            "SOURCE_SYSTEM_ID",
            substring(regexp_replace(col("CLAIM_ID"), r"[A-Z]+_", ""), 1, 10),
        )

    @staticmethod
    def add_transaction_type(df: DataFrame) -> DataFrame:
        """
        Adds the 'TRANSACTION_TYPE' column to the DataFrame.
        'TRANSACTION_TYPE' is set based on the value of 'CLAIM_TYPE' column.
        Args:
            df (DataFrame): The DataFrame to which the column will be added.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.withColumn(
            "TRANSACTION_TYPE",
            when(col("CLAIM_TYPE") == 2, "Corporate")
            .when(col("CLAIM_TYPE") == 1, "Private")
            .otherwise("Unknown"),
        )

    @staticmethod
    def add_transaction_direction(df: DataFrame) -> DataFrame:
        """
        Adds the 'TRANSACTION_DIRECTION' column to the DataFrame.
        'TRANSACTION_DIRECTION' is set based on the value of 'CLAIM_ID' column.
        Args:
            df (DataFrame): The DataFrame to which the column will be added.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.withColumn(
            "TRANSACTION_DIRECTION",
            when(col("CLAIM_ID").startswith("CL"), "COINSURANCE")
            .when(col("CLAIM_ID").startswith("RX"), "REINSURANCE")
            .otherwise(""),
        )

    @staticmethod
    def add_business_date(df: DataFrame) -> DataFrame:
        """
        Adds the 'BUSINESS_DATE' column to the DataFrame.
        'BUSINESS_DATE' is extracted from the 'DATE_OF_LOSS' column.
        Args:
            df (DataFrame): The DataFrame to which the column will be added.
        Returns:
            DataFrame: A new DataFrame with the added column.
        """
        return df.withColumn(
            "BUSINESS_DATE", to_date(col("DATE_OF_LOSS"), "dd.MM.yyyy")
        )

    @staticmethod
    def add_creation_date(df: DataFrame) -> DataFrame:
        """
        Adds a new column "CREATION_DATE" to the DataFrame with timestamp values parsed from a string format "dd.MM.yyyy HH:mm".
        Args:
            df (DataFrame): The input DataFrame to which the column needs to be added.
        Returns:
            DataFrame: The transformed DataFrame with the new column "CREATION_DATE".
        """
        return df.withColumn(
            "CREATION_DATE", to_timestamp(col("CREATION_DATE"), "dd.MM.yyyy HH:mm")
        )

    @staticmethod
    def add_system_timestamp(df: DataFrame) -> DataFrame:
        """
        Adds a new column "SYSTEM_TIMESTAMP" to the DataFrame with the current system timestamp.
        Args:
            df (DataFrame): The input DataFrame to which the column needs to be added.
        Returns:
            DataFrame: The transformed DataFrame with the new column "SYSTEM_TIMESTAMP".
        """
        return df.withColumn("SYSTEM_TIMESTAMP", current_timestamp())

    @staticmethod
    def add_conformed_value(df: DataFrame) -> DataFrame:
        """
        Adds a new column "CONFORMED_VALUE" to the DataFrame with values renamed from the column "AMOUNT".
        Args:
            df (DataFrame): The input DataFrame to which the column needs to be added.
        Returns:
            DataFrame: The transformed DataFrame with the new column "CONFORMED_VALUE".
        """
        return df.withColumn("CONFORMED_VALUE", col("AMOUNT").cast("decimal(16,5)"))

    @staticmethod
    def add_nse_id(df: DataFrame, api_key: str) -> DataFrame:
        """
        Adds a new column "NSE_ID" to the DataFrame with unique ID values mapped from the "CLAIM_ID" column via a REST API call.
        Args:
            df (DataFrame): The input DataFrame to which the column needs to be added.
            api_key (str): The API key used to access the REST API.
        Returns:
            DataFrame: The transformed DataFrame with the new column "NSE_ID".
        """

        @udf
        def get_nse_id(claim_id: str) -> str:
            """
            Function is created to Map CLAIM_ID to a unique id via rest API call for every input row.
            """
            url = api_key + claim_id
            try:
                response = requests.get(url)
                response.raise_for_status()
                nse_id = response.json()["Digest"]
                return nse_id
            except ConnectionError as e:
                raise ValueError(
                    f"Connection error when getting NSE ID for claim ID {claim_id}: {e}"
                )
            except RequestException as e:
                raise ValueError(f"Error getting NSE ID for claim ID {claim_id}: {e}")

        return df.withColumn("NSE_ID", get_nse_id(col("CLAIM_ID")))

    @staticmethod
    def arrange_schema(df: DataFrame, schema: StructType) -> DataFrame:
        """
        Transforms and rearranges the columns according to the desired schema.
        Args:
            df (DataFrame): The input DataFrame to be transformed.
            schema (StructType): The schema that contains the desired column names and order.
        Returns:
            DataFrame: The transformed DataFrame with the columns renamed and rearranged.
        """
        column_names = schema.fieldNames()
        casted_columns = [col(c).cast(schema[c].dataType) for c in column_names]
        df = df.select(
            *[
                casted_columns[i].alias(column_names[i])
                for i in range(len(column_names))
            ]
        )
        df = spark.createDataFrame(df.rdd, schema)
        return df

    def transform_data(self) -> DataFrame:
        """
        Transforms the output data by applying a series of transformations to the input data.
        Returns:
            transformed_df (DataFrame): the transformed DataFrame.
        """
        contracts_df, claim_df = self.read_data()
        transformed_df = (
            claim_df.transform(
                lambda df: self.add_contract_source_system_id(claim_df, contracts_df)
            )
            .transform(self.add_contract_source_system)
            .transform(self.add_source_system_id)
            .transform(self.add_transaction_type)
            .transform(self.add_transaction_direction)
            .transform(self.add_business_date)
            .transform(self.add_creation_date)
            .transform(self.add_system_timestamp)
            .transform(self.add_conformed_value)
            .transform(self.add_nse_id, api_key=self.api)
            .transform(self.arrange_schema, schema=self.output_schema)
        )

        return transformed_df

    def write_output(
        self,
        df=None,
        output_path=None,
        partitions=["CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID"],
        mode="overwrite",
    ):
        """
        Writes the output DataFrame to a Delta table.
        Args:
            df (DataFrame, optional): the input DataFrame. If not provided, the transform_data() method is called.
            output_path (str): the path to the output Delta table.
            partitions (list of str, optional): the columns to use for partitioning.
            mode (str, optional): the mode to use when writing to the Delta table.
        Returns:
            message (str): a success message indicating the location of the output Delta table.
        """
        if output_path is None:
            output_path = self.output_path
        if df is None:
            df = self.transform_data()
        if df.schema != self.output_schema:
            raise Exception(
                f"Provided schema {df.schema} is not the same as required {self.output_schema}"
            )
        df.write.format("delta").mode(mode).partitionBy(partitions).save(output_path)
        return f"Successfully wrote file to {output_path}"
