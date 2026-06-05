import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("request_model").getOrCreate()

url = "http://127.0.0.1:8000/predict"

def call_api(row):
    """
    I'm not 100% sure so I've just copied the example app sent to me when making requests. Although
    all my predictions/probabilities are coming back as essentially zero, I'm not exactly sure why
    but it seems like the requests themselves are working. I have a feeling either the dataset isn't
    sufficient enough or I've missed something.
    """


    payload = {
        "transaction_count": row["transaction_count"],
        "total_debit": row["total_debit"],
        "total_credit": row["total_credit"],
        "avg_amount": row["average_amount_pounds"],
        "rent": row["rent"],
        "tesco": row["tesco"],
        "netflix": row["netflix"],
        "payroll": row["acme_employee"],
        "bonus": row["bonus"]
    }

    try:
        res = requests.post("http://127.0.0.1:8000/predict", json=payload)
        return (row["customer_id"], res.json()["prediction"], res.json()["probability"])
    except Exception as e:
        return (row["customer_id"], str(e), None)

if __name__ == "__main__":
    df = spark.read.csv("artifacts/training_set.csv", header=True, inferSchema=True)
    # toLocalIterator stops us overloading a driver.
    for row in df.toLocalIterator():
        result = call_api(row)
        print(f"Customer ID: {result[0]}, Prediction: {result[1]}, Probability: {result[2]}")
