import os

import delta
import mimesis
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def get_spark() -> SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("TestApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    dir = os.getcwd()
    spark = get_spark()

    # Read the JSON data
    data = spark.read.option("multiline","true").json("eat_by_date/test.json")

    data.select("category").show()
    data.write.format("delta").save('delta_lake')

if __name__ == "__main__":
    main()