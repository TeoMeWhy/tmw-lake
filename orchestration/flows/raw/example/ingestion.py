# %%
from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
                        .appName("Ingestao RAW")
                        .master("spark://spark-master:7077")
                        .getOrCreate())

data = [("A", 1), ("B", 2), ("C", 3)]
df = spark.createDataFrame(data, ["letra", "numero"])

df.write.mode("overwrite").parquet("s3a://raw/example/ingestion")

spark.stop()