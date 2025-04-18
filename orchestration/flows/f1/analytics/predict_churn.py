# %%
import datetime
import pandas as pd
from pyspark.sql import SparkSession
import mlflow


spark = (SparkSession.builder
                     .appName("Analytics")
                     .master("local[4]")
                     .getOrCreate())


mlflow.set_tracking_uri("http://mlflow:5000")

models = mlflow.search_registered_models(filter_string="name='f1_churn'")[0]
version = max([int(i.version) for i in models.latest_versions])
dt_model = max([int(i.creation_timestamp) for i in models.latest_versions])
dt_model = datetime.datetime.fromtimestamp(dt_model/1000)

# %%
model = mlflow.sklearn.load_model(f"models:/f1_churn/{version}")

# %%
df = (spark.read
           .format("delta")
           .load("s3a://silver/f1/fs_drivers")
           .filter("dtRef > '2025-01-01'")
           .filter("dtLastSeen > '2025-01-01'")
           .toPandas())

# %%
features = model.feature_names_in_
df["pctProbaChurn"] = model.predict_proba(df[features])[:,1]

# %%
df["dtModel"] = dt_model
df["nrModelVersion"] = version

columns = ['idDriver', 'idTeam', 'dtModel', 'nrModelVersion', 'dtRef', 'pctProbaChurn']

# %%

sdf = spark.createDataFrame(df[columns])

(sdf.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"nrModelVersion = {version}")
    .save("s3a://silver/f1/model_churn"))
