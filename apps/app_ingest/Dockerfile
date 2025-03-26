FROM bitnami/spark:latest

COPY main.py /app/main.py

CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/app/main.py"]
