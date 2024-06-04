"""Dag is a demo designed to test fs connection."""
import pendulum
from airflow.decorators import dag, task
from airflow.hooks.filesystem import FSHook


@dag(
    "demo_fs_connection",
    start_date=pendulum.today(),
    schedule="@once",
    catchup=False,
    tags=["demo"]
)
def demo_fs_connection():
    """
    Test fs connection
    """
    @task()
    def get_path() -> str:
        fs_hook = FSHook()
        return fs_hook.get_path()

    @task()
    def read_file(path: str, file: str):
        with open(f"{path}/file/{file}", encoding="utf-8") as f:
            line = f.readline()
            print(line)

    file_path = get_path()
    read_file(file_path, "test.log")


dag_object = demo_fs_connection()


if __name__ == "__main__":
    dag_object.test()
