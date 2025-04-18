
# %%
import datetime
import pandas as pd
from pyspark.sql import SparkSession
import mlflow
import matplotlib.pyplot as plt


spark = (SparkSession.builder
                     .appName("Analytics")
                     .master("local[4]")
                     .getOrCreate())

# %%

df = spark.read.format("delta").load("s3a://silver/f1/model_churn").toPandas()
df

# %%

# %%
df['color'] = df['idDriver'].replace({
    "albon":"",
    "alonso":"#008020",
    "antonelli":"#89d3bf",
    "bearman":"#fff4f4",
    "bortoleto":"#38ff45",
    "doohan":"#ff99f9",
    "hamilton":"#fc9292",
    "hulkenberg":"#00a315",
    "lawson":"#84bbff",
    "leclerc":"#ff0000",
    "max_verstappen":"#fff82d",
    "norris":"#ffd699",
    "piastri":"#ff9d00",
    "ocon":"#ff9999",
    "russell":"#bed6c5",
    "stroll":"#1b562a",
    "tsunoda":"#796b9b",
})

# %%
columns = ['idDriver','dtRef', 'pctProbaChurn', 'color']
df[columns].sort_values(['idDriver','dtRef'])

# %%
df_latest = df[df["dtRef"]==df["dtRef"].max()].sort_values('pctProbaChurn', ascending=False)
df_latest_top_churn = df_latest[columns].head(5)
df_latest_botton_churn = df_latest[columns].tail(5)
df_latest_botton_churn
# %%

drivers = df_latest_top_churn['idDriver'].unique()

plt.figure(figsize=(9,8), dpi=400)

for i in drivers:
    data = df[df['idDriver']==i].sort_values("dtRef")
    plt.plot(data["dtRef"], data["pctProbaChurn"], 'o-', color=data["color"].iloc[0])
    print(i)

plt.legend(drivers)
plt.xlabel("Data do cálculo")
plt.ylabel("Probabilidade")
plt.suptitle("Probabilidade de não estar presente na próxima temporada")
plt.title("TOP 5 mais provaveis")
plt.grid(True)

# mplcyberpunk.make_lines_glow()

plt.show()
# plt.savefig("top_churners.png")


# %%
drivers = df_latest_botton_churn['idDriver'].unique()

plt.figure(figsize=(9,8), dpi=400)

for i in drivers:
    data = df[df['idDriver']==i].sort_values("dtRef")
    plt.plot(data["dtRef"], data["pctProbaChurn"], 'o-', color=data["color"].iloc[0])

plt.legend(drivers)
plt.xlabel("Data do cálculo")
plt.ylabel("Probabilidade")
plt.suptitle("Probabilidade de não estar presente na próxima temporada")
plt.title("TOP 5 menos provaveis")
plt.grid(True)
# mplcyberpunk.make_lines_glow()

plt.savefig("bottom_churners.png")
plt.show()
