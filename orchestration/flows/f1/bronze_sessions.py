from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
                        .appName("Ingestao Bronze")
                        .master("local[4]")
                        .getOrCreate())


df = spark.read.format("json").load("s3a://raw/f1/results")
df.createOrReplaceTempView("f1_results")


print(df.toPandas().head())

query = """

WITH f1_results_rn AS (
    SELECT *,
            row_number() OVER (PARTITION BY date, location, DriverId, event_name ORDER BY dt_now DESC) AS rn
    FROM f1_results
),

updated AS (
    SELECT *
    FROM f1_results_rn
    WHERE rn=1
    ORDER BY date, DriverNumber
)

SELECT * FROM updated

"""

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("s3a://bronze/f1/results")
)

spark.stop()
