import sys
from decimal import Decimal

import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.append("..")  # Folder structure is one above for imports

from data_prep.transform import Transform as t
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, FloatType
from pyspark.sql.functions import col

def schema():
    schema_default = StructType([
    StructField("customer_id", StringType(), True),
    StructField("defaulted_within_90d", IntegerType(), True)
    ])
    schema_txn = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("txn_timestamp", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("txn_type", StringType(), True),
        StructField("description", StringType(), True)
    ])
    schema_compare = StructType([
        StructField("customer_id", StringType(), True),
        StructField("transaction_count", IntegerType(), True),
        StructField("total_debit", DecimalType(18,2), True),
        StructField("total_credit", DecimalType(18,2), True),
        StructField("average_amount_pounds", DecimalType(18,2), True),
        StructField("max_amount_pounds", DecimalType(18,2), True),
        StructField("min_amount_pounds", DecimalType(18,2), True),
        StructField("first_transaction_timestamp", StringType(), True),
        StructField("income_count", IntegerType(), True),
        StructField("acme_employee", IntegerType(), True),
        StructField("bonus", IntegerType(), True),
        StructField("rent", IntegerType(), True),
        StructField("tesco", IntegerType(), True),
        StructField("netflix", IntegerType(), True),
        StructField("defaulted_within_90d", IntegerType(), True)
    ])

    return schema_txn, schema_default, schema_compare

def data():
    data_txn = [("T00001","CUST_0001","2025-02-01T08:26:00",2500.00,"credit","ACME LTD PAYROLL FEB"),
            ("T00202","CUST_0001","2025-02-04T00:13:00",-400.00,"debit","AMAZON.COM"),
            ("T05003","CUST_0001","2025-02-11T10:05:00",-931.13,"debit","RENT PAYMENT"),
            ("T50104","CUST_0001","2025-02-11T23:59:59",-118.87,"debit","TESCO.CO.UK"),
        ]
    data_default = [("CUST_0001", 0)]

    return data_txn, data_default

def test_aggregate_txn(spark):

    data_txn, data_default = data()

    data_compare = [("CUST_0001", 4, Decimal("-1450.00"), Decimal("2500.00"), Decimal("262.50"), Decimal("2500.00"), Decimal("-931.13"), "2025-02-01T08:26:00", 1, 1, 0, 1, 1, 0, 0)]

    schema_txn, schema_default, schema_compare = schema()

    # My two data sets
    df_txn = spark.createDataFrame(data_txn, schema_txn)
    df_default = spark.createDataFrame(data_default, schema_default)

    # Ingesting timestamp as a string but conver to timestamp in dataframe
    df_compare = spark.createDataFrame(data_compare, schema_compare).withColumn("first_transaction_timestamp", col("first_transaction_timestamp").cast(TimestampType()))

    df_result = t.aggregate_txn(df_txn, df_default)

    assert df_result.exceptAll(df_compare).count() == 0
    assert df_compare.exceptAll(df_result).count() == 0

def test_clean_description(spark):
    data_description = [("CUST_0001", 1400.98, "ACME LTD PAYROLL FEB"),
            ("CUST_0001", -200.13, "AMAZON.COM"),
            ("CUST_0001", -941.00, "RENT PAYMENT"),
            ("CUST_0002", -31.10, "TESCO.CO.UK"),
            ("CUST_0002", -15.99, "NETFLIX.CO.UK"),
            ("CUST_0003", -500.00, "COSTCO"),
            ("CUST_0003", 100.00, "BONUS PAYMENT"),
        ]
    expected_data = [("CUST_0001","expense,rent,salary"),
                     ("CUST_0002","netflix,tesco"),
                     ("CUST_0003","bonus,expense")]
    
    schema_description = StructType([
        StructField("customer_id", StringType(), True),
        StructField("amount", FloatType(), True),
        StructField("description", StringType(), True)
    ])

    schema_compare = StructType([
        StructField("customer_id", StringType(), True),
        StructField("categories", StringType(), True)
    ])

    df_description = spark.createDataFrame(data_description, schema_description)
    df_compare = spark.createDataFrame(expected_data, schema_compare)

    df_result = t.clean_description(df_description)

    df_result.show()
    df_compare.show()

    assert df_result.exceptAll(df_compare).count() == 0
    assert df_compare.exceptAll(df_result).count() == 0

    return