from prefect import task, flow
from prefect_shell import ShellOperation


@task
def run(step:str):
    cmd = f"docker exec executer python orchestration/flows/f1/{step}.py"
    op = ShellOperation(commands=[cmd],stream_output=True)
    result = op.run()
    return result


@flow(name="Ingestão F1")
def ingestao_flow():
    run(step="raw_sessions")
    run(step="bronze_sessions")
    run(step="silver_sessions")


if __name__ == "__main__":
    ingestao_flow()