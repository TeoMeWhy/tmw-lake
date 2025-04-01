from prefect import task, flow
from prefect_shell import ShellOperation


@task
def run():
    op = ShellOperation(
        commands=["docker exec executer python flows/raw/example/ingestion.py"],
        stream_output=True,
    )
    result = op.run()
    return result


@flow
def ingestao_flow():
    run()

if __name__ == "__main__":
    ingestao_flow()