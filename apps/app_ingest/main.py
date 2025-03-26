import time

from pyspark.sql import SparkSession

def main():

    spark = SparkSession.builder \
        .appName("Ingestao RAW") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Simulação de dados
    data = [("sensor01", 23.4), ("sensor02", 19.8), ("sensor03", 25.1)]
    df = spark.createDataFrame(data, ["sensor_id", "temperatura"])

    # Escreve os dados no bucket 'raw'
    df.write.mode("append").parquet("s3a://raw/temperaturas/")

    print("✅ Dados salvos no bucket 'raw'")

if __name__ == "__main__" :
    while True:
        main()
        time.sleep(10)

