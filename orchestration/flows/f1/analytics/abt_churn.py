import os

from pyspark.sql import SparkSession
import delta


FILE_PATH = os.path.abspath(__file__)
DIR_PATH = os.path.dirname(FILE_PATH)

spark = (SparkSession.builder
                     .appName("Analytics")
                     .master("local[4]")
                     .getOrCreate())


spark.read.format("delta").load("s3a://silver/f1/fs_drivers").createOrReplaceTempView("fs_drivers")
spark.read.format("delta").load("s3a://silver/f1/results").createOrReplaceTempView("results")

with open(os.path.join(DIR_PATH, "abt_churn.sql")) as open_file:
    query = open_file.read()

(spark.sql(query)
      .coalesce(1)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overWriteSchema", "true")
      .save("s3a://analytics/f1/abt_churn")
)

table = delta.DeltaTable.forPath(spark, "s3a://analytics/f1/abt_churn")
table.vacuum()

spark.stop()