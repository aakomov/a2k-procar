from prefect import flow, task

@task
def say_hello(name: str) -> str:
    print(f"Привет, {name}!")
    return f"Hello, {name}!"

@flow(name="hello-flow")
def hello_flow():
    result = say_hello("Prefect")
    print(f"Результат задачи: {result}")

if __name__ == "__main__":
    hello_flow()
