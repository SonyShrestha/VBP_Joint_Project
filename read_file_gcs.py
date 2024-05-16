from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GCS Files Read") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("google.cloud.auth.service.account.json.keyfile", "C:\\UPC\\BDM\\Project\\VBP_Joint_Project\\gcs_config.json") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet('gs://formatted_zone/customers')

df.show()

spark.stop()

