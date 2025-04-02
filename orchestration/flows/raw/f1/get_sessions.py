# %%
import os

import boto3
from botocore.client import Config
import requests

from ingestor import RawIngestor

MINIO_URI = os.getenv("MINIO_URI")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_DATA_PATH = os.getenv("MINIO_DATA_PATH")


def get_sessions(**kwargs):
    url = "https://api.openf1.org/v1/sessions"
    req = requests.get(url, params=kwargs)
    if req.status_code != 200:
        return dict()
    
    return req.json()


class IngestorSessions(RawIngestor):
    def __init__(self, tablename, s3_client):
        super().__init__(tablename, s3_client)

    def get_data(self,**kwargs):
        return get_sessions(**kwargs)


def main():
    s3 = boto3.resource(
        's3',
        endpoint_url=MINIO_URI,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version='s3v4'),
    )

    sessions = IngestorSessions("sessions", s3)
    sessions.run([
        {"year": 2023},
        {"year": 2024},
        {"year": 2025},
    ])


if __name__ == "__main__":
    main()

