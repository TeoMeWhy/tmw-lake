from prefect import task, flow
from prefect_shell import ShellOperation


@task
def run():
    cmd = "docker exec executer python flows/raw/f1/get_sessions.py"
    op = ShellOperation(commands=[cmd],stream_output=True)
    result = op.run()
    return result


@flow
def ingestao_flow():
    run()

if __name__ == "__main__":
    ingestao_flow()