import datetime

from pyspark.sql import SparkSession

import streamlit as st

# %%

@st.cache_resource(ttl=datetime.timedelta(hours=1))
def connect_spark():
    spark = (SparkSession.builder
                         .appName("Streamlit App")
                         .master("local[2]")
                         .getOrCreate())
    return spark


@st.cache_data(ttl=datetime.timedelta(hours=6))
def get_predictions(_spark):
    return (_spark.read
                 .format("delta")
                 .load("s3a://analytics/f1/model_churn")
                 .toPandas())
    
    