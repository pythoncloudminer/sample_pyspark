from pyspark.sql import SparkSession
from utils import log_message

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Simple PySpark App") \
        .getOrCreate()

    # Example data
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["Name", "Age"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Log a message
    log_message("DataFrame created successfully")

    # Perform transformation: Filter rows where age > 30
    filtered_df = df.filter(df["Age"] > 30)

    # Show the result
    filtered_df.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
