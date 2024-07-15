"""Dag is a demo designed to test dataset consumer."""

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task

INSTRUCTIONS = Dataset("file://localhost/airflow/include/cocktail_instructions.txt")
INFO = Dataset("file://localhost/airflow/include/cocktail_info.txt")


@dag(
    "demo_datasets_consumer",
    start_date=pendulum.today(),
    schedule=[INSTRUCTIONS, INFO],
    catchup=False,
    tags=["demo"],
)
def demo_datasets_consumer():
    """
    Test datasets consumer
    """

    @task()
    def get_path() -> str:
        from airflow.hooks.filesystem import FSHook

        fs_hook = FSHook()
        return fs_hook.get_path()

    @task
    def read_about_cocktail(path: str) -> list:
        cocktail = []
        with open(f"{path}/cocktail_instructions.txt", "r", encoding="utf-8") as f:
            contents = f.readlines()
            cocktail.append(contents)

        return [item for sublist in cocktail for item in sublist]

    path = get_path()

    read_about_cocktail(path)


dag_object = demo_datasets_consumer()


if __name__ == "__main__":
    dag_object.test()
