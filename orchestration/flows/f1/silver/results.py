import os

from pyspark.sql import SparkSession

import orchestration.flows.f1.silver.utils as utils

FILE_PATH = os.path.abspath(__file__)
DIR_PATH = os.path.dirname(FILE_PATH)

QUERY_FILE = "results.sql"
QUERY_PATH = os.path.join(DIR_PATH, QUERY_FILE)


spark = (SparkSession.builder
                        .appName("Ingestao Silver")
                        .master("local[4]")
                        .getOrCreate())


df = spark.read.format("delta").load("s3a://bronze/f1/results")
df.createOrReplaceTempView("f1_results")

query = utils.import_query(QUERY_PATH)

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("s3a://silver/f1/results")
)

spark.stop()
