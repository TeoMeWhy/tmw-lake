from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
                        .appName("Ingestao Silver")
                        .master("local[4]")
                        .getOrCreate())


df = spark.read.format("delta").load("s3a://bronze/f1/results")
df.createOrReplaceTempView("f1_results")

query = """

WITH renamed AS (

    SELECT
            Abbreviation AS descDriverAbreviation,
            DriverId As idDriver,
            DriverNumber AS nrDriver,
            FirstName AS descDriverName,
            FullName AS descDriverFullName,
            int(ClassifiedPosition) AS nrClassifiedPosition,
            int(GridPosition) AS nrGridPosition,
            int(Position) AS nrPosition,
            int(Points) AS nrPoints,
            TeamId AS idTeam,
            TeamName AS descTeamName,
            Time AS descTimeDuration,
            country AS descCountryName,
            location AS descSessionLocation,
            date(date) AS dtSession,
            event_name AS descSession
            
    FROM f1_results
)

SELECT * FROM renamed
"""

(spark.sql(query)
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save("s3a://silver/f1/results")
)

spark.stop()
