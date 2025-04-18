import os
from tqdm import tqdm
from pyspark.sql import SparkSession

import orchestration.flows.f1.silver.utils as utils

FILE_PATH = os.path.abspath(__file__)
DIR_PATH = os.path.dirname(FILE_PATH)

QUERY_FILE = "fs_drivers.sql"
QUERY_PATH = os.path.join(DIR_PATH, QUERY_FILE)

spark = (SparkSession.builder
                     .appName("Ingestao Silver")
                     .master("local[4]")
                     .getOrCreate())


results = spark.read.format("delta").load("s3a://silver/f1/results")
results.createOrReplaceTempView("results")

sessions = spark.read.format("delta").load("s3a://silver/f1/sessions")
sessions.createOrReplaceTempView("sessions")

lines = (results.filter("descSession = 'Race'")
                .select("dtSession")
                .distinct()
                .orderBy("dtSession")
                .collect())

values = [i[0] for i in lines]

query = utils.import_query(QUERY_PATH)

for i in tqdm(values[-2:]):

    (spark.sql(query.format(date=i))
          .write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", f"dtRef = '{i}'")
          .save("s3a://silver/f1/fs_drivers"))

spark.stop()
