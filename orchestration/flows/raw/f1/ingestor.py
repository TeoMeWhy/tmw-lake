import datetime
import json


class RawIngestor:

    def __init__(self, tablename, s3_client):
        self.tablename = tablename
        self.s3_client = s3_client

    def get_data(**kwargs):
        pass

    def save_data(self, data, **kwargs):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")
        data = json.dumps(data).encode('utf-8')

        values = "_".join([str(i) for i in kwargs.values()])

        path = f"f1/{self.tablename}/{values}_{now}.json"
        (self.s3_client
             .Object("raw", path)
             .put(Body=data, ContentType='application/json'))

    def run(self, values: list[dict]):
        for v in values:
            data = self.get_data(**v)
            if len(data) == 0:
                continue

            self.save_data(data, **v)

