# %%
import pandas as pd
import json

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from rich.progress import track
import schedule
import time
import datetime

import gc
import os
import dotenv
dotenv.load_dotenv()

MYSQL_URI = os.getenv("MYSQL_URI")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

with open("./tables_collect.json", "r") as f:
    tables = json.load(f)

# %%
def import_query(path):
    with open(path, "r") as f:
        query = f.read()
    return query


def setup_tables(spark):
    with open("./tables_collect.json", "r") as f:
        tables_collect = json.load(f)

    for table in track(tables_collect, description="Setting up tables..."):
        database = table["database"]
        tablename = table["table"]
        layer = table["layer"]
        try:
            (spark.read
                  .format("delta")
                  .load(f"./data/{layer}/{database}/{tablename}")
                  .createOrReplaceTempView(f"{layer}_{database}_{tablename}"))
        except Exception as e:
            print(f"Erro ao carregar {database}.{tablename}: {e}")


def mysql_to_bronze(spark, database, tablename):

    jdbc_url = f"jdbc:mysql://{MYSQL_URI}/{database}"

    db_properties = {
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df_mysql = (spark.read
                     .format("jdbc")
                     .option("url", jdbc_url)
                     .option("dbtable", tablename)
                     .options(**db_properties)
                     .load())

    (df_mysql.coalesce(1)
             .write
             .format("delta")
             .option("overwriteSchema", "true")
             .mode("overwrite")
             .save(f"./data/bronze/{database}/{tablename}"))
    
    delta_tbl = DeltaTable.forPath(spark, f"./data/bronze/{database}/{tablename}")
    delta_tbl.vacuum(retentionHours=24)
    
    del df_mysql
    del delta_tbl
    gc.collect()


def process_table_by_query(spark, query_name, layer, database, date_start, date_end):
    dates = pd.date_range(start=date_start, end=date_end, freq="D").strftime("%Y-%m-%d").tolist()
    query = import_query(f"{layer}/{query_name}.sql")

    for i in track(dates, description=f"processando tabela {query_name}..."):
        query_fmt = query.format(date=i)
        
        (spark.sql(query_fmt)
              .coalesce(1)
              .write
              .format("delta")
              .mode("overwrite")
              .partitionBy("dtRef")
              .option("replaceWhere", f"dtRef = '{i}'")
              .save(f"./data/{layer}/{database}/{query_name}"))

    deltatable = DeltaTable.forPath(spark, f"./data/{layer}/{database}/{query_name}")
    deltatable.vacuum(retentionHours=24)
    del deltatable
    gc.collect()


def lake_to_sql(spark, query_name, database, table,):
    setup_tables(spark)
    
    jdbc_url = f"jdbc:mysql://{MYSQL_URI}/{database}"

    db_properties = {
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    query = import_query(f"exporter/{query_name}.sql")

    (spark.sql(query)
          .write
          .format("jdbc")
          .option("url", jdbc_url)
          .option("dbtable", table)
          .options(**db_properties)
          .option("truncate", "true")
          .mode("overwrite")
          .save())


def run_bronze(spark, tables):
    setup_tables(spark)
    for i in track(tables, description="Processando tabelas bronze..."):
        if i["layer"]=='bronze':
            mysql_to_bronze(spark, i['database'], i['table'])


def run_silver(spark, tables):
    now = datetime.datetime.now()
    past = now - datetime.timedelta(days=1)
    setup_tables(spark)
    for i in track(tables, "Processando tabelas silver..."):
        if i['layer'] == 'silver':
            process_table_by_query(spark,i['table'],i['layer'],i['database'],past.strftime("%Y-%m-%d"),now.strftime("%Y-%m-%d"))
        
# %%

builder = (SparkSession.builder
                       .appName("TMW-Lake-Test")
                       .master("local[2]")
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                       .config("spark.sql.warehouse.dir", "spark-warehouse")
                       .config("spark.driver.memory", "2g")
                       .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                       .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
)

spark = (configure_spark_with_delta_pip(builder,extra_packages=["com.mysql:mysql-connector-j:8.3.0"]).getOrCreate())

schedule.every(4).hour.at(":00").do(run_bronze,spark=spark,tables=tables)
schedule.every(4).hour.at(":15").do(run_silver,spark=spark,tables=tables)
schedule.every(4).hour.at(":30").do(lake_to_sql,spark=spark,query_name="fs_all",database="feature_store",table="tmw_user")

while True:
    schedule.run_pending()
    gc.collect()
    spark.sparkContext._jvm.System.gc()
    time.sleep(1)