from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, col, count, when, avg, max as _max, min as _min, regexp_replace, lower
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("transform").getOrCreate()

df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)

# At this point, the decimal values of amount cause a floating point error for CUST-0001 as I allured to in the data_check.py file.
#transactions_df = transactions_df.groupBy("customer_id").agg(_sum("amount"))

def write_csv(df, path):
    """
        Something with my hadoop doesn't like this so I'm manually going to save the file.

        To resolve this, I'm just going to paste the result into claude to spit out the csv format and manually save.

        Obviously in a production environment (and staging) with pii data, I wouldn't do this.
    """
    df.coalesce(1).write.csv(path, header=True, mode="overwrite")
    return({"success": True})

# To resolve this floating point we instead can do the following. It's a little messy and I'd suggest just keeping everything all the way through the data stream.
def aggregate_txn(df):
    df = df.alias("txn")
    default_df = spark.read.csv("data/labels.csv", header=True, inferSchema=True).alias("label")
    return(
        df
        .withColumn("amount_pence", (col("amount").cast(DecimalType(18, 2))*100).cast("long"))
        .withColumn("amount_pounds", col("amount_pence")/100)
        .groupBy("customer_id")
        .agg(
            _sum("amount_pence").alias("amount_pence"),
            count("transaction_id").alias("transaction_count"),
            _sum(when(col("amount") < 0, 1).otherwise(0)).alias("debit_count"),
            _sum(when(col("amount") >= 0, 1).otherwise(0)).alias("credit_count"),
            avg(col("amount_pounds").cast(DecimalType(18,2))).alias("average_amount_pounds"),
            _max(col("amount_pounds").cast(DecimalType(18,2))).alias("max_amount_pounds"),
            _min(col("amount_pounds").cast(DecimalType(18,2))).alias("min_amount_pounds"),
            _min("txn_timestamp").alias("first_transaction_timestamp"),
            _sum(
                when(
                    col("description").like("%PAYROLL%") |
                    col("description").like("%BONUS%") |
                    col("description").like("%PAYOUT%"),
                    1)
                .otherwise(0)
            ).alias("income_count"),
            # I would cast as boolean but I will keep as bit integers for now in case it causes problems with the model
            _max(when(col("description").like("%ACME%"), 1).otherwise(0)).alias("acme_employee")
        )
        .join(default_df, on="customer_id", how="left")
        # I would cast defaulted as boolean but I will keep as bit integers for now in case it causes problems with the model)
        .drop("label.customer_id")
        .drop("amount_pence")
        .orderBy("customer_id")
    )

aggregate_txn(df).show()
#write_csv(new_df, "../artifacts/")


def clean_description(df):
    # https://stackoverflow.com/questions/66808400/regex-pattern-to-remove-numeric-value-from-words-in-pyspark
    # and https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html helped get mee in a good spot
    return (
        df.withColumn(
            "description_clean",
            lower(regexp_replace(col("description"), r"\d+|\.(COM|CO\.UK)\b", ""))
        )
        .select(
            when(
                col("description_clean").like("%payroll%") |
                col("description_clean").like("%bonus%") |
                col("description_clean").like("%payout%"), "salary"
            ).otherwise(
                when(
                    col("description_clean").like("%rent%"), "rent"
                ).otherwise(
                    when(
                        col("amount") < 0, "expense"
                    ).otherwise("other")
                ) # To save time, anything else is "other", not going to be super accurate
            ).alias("description_category")
        )
    )    

clean_description(df).show()

