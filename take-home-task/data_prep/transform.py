from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, col, count, when, avg, max as _max, min as _min, regexp_replace, lower, concat_ws, collect_set, array_sort
from pyspark.sql.types import DecimalType


spark = SparkSession.builder.appName("transform").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 1)

df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)

# At this point, the decimal values of amount cause a floating point error for CUST-0001 as I allured to in the data_check.py file.
#transactions_df = transactions_df.groupBy("customer_id").agg(_sum("amount"))
class Transform:
    @staticmethod
    def write_csv(df, path):
        """
            Something with my hadoop doesn't like this so I'm manually going to save the file.

            To resolve this, I'm just going to paste the result into claude to spit out the csv format and manually save.

            Obviously in a production environment (and staging) with pii data, I wouldn't do this.
        """
        df.coalesce(1).write.csv(path, header=True, mode="overwrite")
        return({"success": True})

    # To resolve this floating point we instead can do the following. It's a little messy and I'd suggest just keeping everything all the way through the data stream.
    @staticmethod
    def aggregate_txn(df, default_df=None):
        """
            This function is to gather a bunch of aggregations per customer that are interesting/note worthy.

            The function takes a transactions dataframe and an optional default_df and outputs a dataframe
            of shape one row per customer.
        """
        df = df.alias("txn")
        # This allows me to keep the input clear but also defaults if empty to a fallback file.
        if default_df is None:
            default_df = spark.read.csv("data/labels.csv", header=True, inferSchema=True).alias("label")
        return(
            df
            .withColumn("amount_pence", (col("amount").cast(DecimalType(18, 2))*100).cast("long"))
            .withColumn("amount_pounds", col("amount_pence")/100)
            .groupBy("customer_id")
            .agg(
                _sum("amount_pence").cast("int").alias("amount_pence"),
                count("transaction_id").cast("int").alias("transaction_count"),
                _sum(when(col("amount") < 0, col("amount").cast(DecimalType(18,2))).otherwise(0)).cast(DecimalType(18,2)).alias("total_debit"),
                _sum(when(col("amount") >= 0, col("amount").cast(DecimalType(18,2))).otherwise(0)).cast(DecimalType(18,2)).alias("total_credit"),
                avg(col("amount_pounds").cast(DecimalType(18,2))).cast(DecimalType(18,2)).alias("average_amount_pounds"),
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
                ).cast("int").alias("income_count"),
                # I would cast as boolean but I will keep as bit integers for now in case it causes problems with the model
                _max(when(col("description").like("%ACME LTD%"), 1).otherwise(0)).alias("acme_employee"),
                _max(when(col("description").like("%BONUS%"), 1).otherwise(0)).alias("bonus"),
                _max(when(col("description").like("%RENT%"), 1).otherwise(0)).alias("rent"),
                _max(when(col("description").like("%TESCO%"), 1).otherwise(0)).alias("tesco"),
                _max(when(col("description").like("%NETFLIX%"), 1).otherwise(0)).alias("netflix")
            )
            .join(default_df, on="customer_id", how="left")
            # I would cast defaulted as boolean but I will keep as bit integers for now in case it causes problems with the model)
            .drop("label.customer_id")
            .drop("amount_pence")
            .orderBy("customer_id")
        )

    @staticmethod
    def clean_description(df):
        """
            This function pivots cleaned descriptors per row into basic patterns aggregated on customer_id.
        """
        # https://stackoverflow.com/questions/66808400/regex-pattern-to-remove-numeric-value-from-words-in-pyspark
        # and https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html helped get mee in a good spot
        df = (df.withColumn(
            "description_clean",
            lower(regexp_replace(col("description"), r"\d+|\.(COM|CO\.UK)\b", ""))
            )
            .select(
                col("customer_id"),
                (when(
                    col("description_clean").rlike("payroll|payout"),
                    "salary"
                )
                .when(
                    col("description_clean").like("%bonus%"),
                    "bonus"
                )
                .when(
                    col("description_clean").like("%rent%"),
                    "rent"
                )
                .when(
                    col("description_clean").like("%tesco%"),
                    "tesco"
                )
                .when(
                    col("description_clean").like("%netflix%"),
                    "netflix"
                )
                .when(
                    col("amount") < 0,
                    "expense"
                )
                .otherwise("other")).alias("description_category") # To save time, anything else is "other", not going to be super accurate
            )
        )
        df = df.groupBy("customer_id").agg(
            concat_ws(",", array_sort(collect_set("description_category"))).alias("categories")
        )
        return(df)

if __name__ == "__main__":
    Transform.aggregate_txn(df).show()
    Transform.clean_description(df).show()

