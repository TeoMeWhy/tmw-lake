from prefect import task, flow
from prefect_shell import ShellOperation


@task
def run(layer:str, table:str):
    cmd = f"docker exec executer python orchestration/flows/f1/{layer}/{table}.py"
    op = ShellOperation(commands=[cmd],stream_output=True)
    result = op.run()
    return result


@flow(name="Ingestão F1")
def ingestao_flow():
    run(layer="raw", table="results")
    run(layer="bronze", table="results")
    run(layer="silver", table="results")
    run(layer="silver", table="sessions")
    run(layer="silver", table="fs_drivers")
    run(layer="analytics", table="abt_churn")
    run(layer="analytics", table="train_churn")
    run(layer="analytics", table="predict_churn")


if __name__ == "__main__":
    ingestao_flow()