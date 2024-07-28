"""Dag is a demo designed to test dataset producer."""

import json

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task

API = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
INSTRUCTIONS = Dataset("file://localhost/airflow/include/cocktail_instructions.txt")
INFO = Dataset("file://localhost/airflow/include/cocktail_info.txt")


@dag(
    "demo_datasets_producer",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_datasets_producer():
    """
    Test datasets producer
    """

    @task()
    def get_cocktail(api: str) -> json:
        import requests

        r = requests.get(api)
        return r.json()

    @task()
    def get_path() -> str:
        from airflow.hooks.filesystem import FSHook

        fs_hook = FSHook()
        return fs_hook.get_path()

    @task(outlets=[INSTRUCTIONS])
    def write_instructions_to_file(response: json, path: str):
        cocktail_name = response["drinks"][0]["strDrink"]
        cocktail_instructions = response["drinks"][0]["strInstructions"]
        msg = f"See how to prepare {cocktail_name}: {cocktail_instructions}"

        with open(f"{path}/cocktail_instructions.txt", "a", encoding="utf-8") as f:
            f.write(msg)

    @task(outlets=[INFO])
    def write_info_to_file(response: json, path: str):
        cocktail_name = response["drinks"][0]["strDrink"]
        cocktail_category = response["drinks"][0]["strCategory"]
        alcohol = response["drinks"][0]["strAlcoholic"]
        msg = f"{cocktail_name} is a(n) {alcohol} cocktail from category {cocktail_category}."

        with open(f"{path}/cocktail_info.txt", "a", encoding="utf-8") as f:
            f.write(msg)

    cocktail = get_cocktail(api=API)
    path = get_path()

    write_instructions_to_file(cocktail, path)
    write_info_to_file(cocktail, path)


dag_object = demo_datasets_producer()


if __name__ == "__main__":
    dag_object.test()
