from prefect import flow

if __name__ == "__main__":

    flow.from_source(
        source="flows/raw/example/",
        entrypoint="flow.py:ingestao_flow"
    ).deploy(
        name="ingestion-example-flow",
        work_pool_name="work-pool-01",
        cron="* * * * *",
    )

