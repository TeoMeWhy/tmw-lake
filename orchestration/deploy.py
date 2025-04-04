from prefect import flow

if __name__ == "__main__":

    # F1 ingestion flow
    flow.from_source(
        source="flows/f1/",
        entrypoint="flow.py:ingestao_flow"
    ).deploy(
        name="ingestion-f1-flow",
        work_pool_name="work-pool-01",
        cron="0 */6 * * *",
    )

