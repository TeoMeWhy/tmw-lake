from prefect import flow

if __name__ == "__main__":

    # Example flow for ingestion
    flow.from_source(
        source="flows/raw/example/",
        entrypoint="flow.py:ingestao_flow"
    ).deploy(
        name="ingestion-example-flow",
        work_pool_name="work-pool-01",
        cron="* * * * *",
    )

    # F1 ingestion flow
    flow.from_source(
        source="flows/raw/f1/",
        entrypoint="flow.py:ingestao_flow"
    ).deploy(
        name="ingestion-f1-flow",
        work_pool_name="work-pool-01",
        cron="0 */6 * * *",
    )

