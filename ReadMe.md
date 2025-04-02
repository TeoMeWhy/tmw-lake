# Datalake Téo Me Why

Criação de um Datalake utilizando ferramentas open-source.

## Uso

Para executar esse projeto localmente, basta executar:

```bash
make
```

Garanta que tenha configurado as seguintes variáveis ambiente:

```
MINIO_URI =
MINIO_ROOT_USER = 
MINIO_ROOT_PASSWORD = 
MINIO_DATA_PATH = 

AWS_ACCESS_KEY_ID = 
AWS_SECRET_ACCESS_KEY = 

PREFECT_ORION_API_HOST = http://prefect-server:4200
PREFECT_API_URL = http://prefect-server:4200/api
PREFECT_DATA_PATH = 
```

## Arquitetura

![Esquema de fluxo de orquestração, ingestão e armazenamento de dados](workflow.jpeg)
