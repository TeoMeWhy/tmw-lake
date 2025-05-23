import os

from pyspark.sql import SparkSession
import delta

import orchestration.flows.f1.silver.utils as utils

FILE_PATH = os.path.abspath(__file__)
DIR_PATH = os.path.dirname(FILE_PATH)

QUERY_FILE = "sessions.sql"
QUERY_PATH = os.path.join(DIR_PATH, QUERY_FILE)


spark = (SparkSession.builder
                        .appName("Ingestao Silver")
                        .master("local[4]")
                        .getOrCreate())


df = spark.read.format("delta").load("s3a://silver/f1/results")
df.createOrReplaceTempView("results")

query = utils.import_query(QUERY_PATH)

(spark.sql(query)
      .coalesce(1)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("s3a://silver/f1/sessions")
)

table = delta.DeltaTable.forPath(spark, "s3a://silver/f1/sessions")
table.vacuum()

spark.stop()
