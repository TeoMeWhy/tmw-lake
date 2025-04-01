# image base
FROM bitnami/spark:3.5.5
 
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["tail", "-f", "/dev/null"]
 
