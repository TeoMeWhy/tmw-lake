# %%

from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
                        .appName("Ingestao RAW")
                        .master("spark://spark-master:7077")
                        .getOrCreate())

df = spark.read.format("json").load("s3a://raw/f1/sessions")
df.createOrReplaceTempView("f1_sessions")

print(df.toPandas().head())

query = """

WITH f1_sessions_rn AS (
    SELECT *,
            row_number() OVER (PARTITION BY session_key ORDER BY date_end DESC) AS rn
    FROM f1_sessions
)

SELECT *
FROM f1_sessions_rn
WHERE rn = 1
"""

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("s3a://bronze/f1/sessions")
)

spark.stop()
