# %%
import datetime
import itertools
import os

import boto3
from botocore.client import Config
import fastf1

from ingestor import RawIngestor

MINIO_URI = os.getenv("MINIO_URI")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_DATA_PATH = os.getenv("MINIO_DATA_PATH")


def get_session(**kwargs):
    print(kwargs)
    try:
        session = fastf1.get_session(**kwargs)
        session.load(telemetry=False, messages=False, laps=False)
        if session.results.shape[0] == 0:
            return []

        results = session.results
        results["date"] = session.event.EventDate.date()
        results["country"] = session.event.Country
        results["location"] = session.event.Location
        results["dt_now"] = datetime.datetime.now()
        results["event_name"] = session.name

        return results.astype(str).to_dict(orient='records')

    except ValueError as e:
        return []
        

class IngestorSessions(RawIngestor):
    def __init__(self, tablename, s3_client):
        super().__init__(tablename, s3_client)

    def get_data(self,**kwargs):
        return get_session(**kwargs)


def main():
    s3 = boto3.resource(
        's3',
        endpoint_url=MINIO_URI,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version='s3v4'),
    )

    gps = list(range(1,25))
    years = list(range(2025, 2026)) #[datetime.datetime.now().year]
    events = ["Race", "Qualifying", "Sprint"]

    values = list(itertools.product(years, gps, events))
    values_dict = [{"year": v[0], "gp": v[1], "identifier": v[2]} for v in values]

    sessions = IngestorSessions("results", s3)
    sessions.run(values_dict)


if __name__ == "__main__":
    main()
