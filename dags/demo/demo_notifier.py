"""Dag is a demo designed to test some notifier."""
from pprint import pprint

import pendulum
from airflow.decorators import dag, task

from airflow.providers.discord.notifications.discord import DiscordNotifier

@dag(
    "demo_notifier",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["demo"],
    on_success_callback=DiscordNotifier("discord_leaves", "Success!"),
    on_failure_callback=DiscordNotifier("discord_leaves", "Failure!"),
)
def demo_notifier():
    """
    Test some notifiers that is
    - discord notifier
    """
    @task()
    def print_kwargs(**kwargs):
        pprint(kwargs)
        print(f"params of kwargs is {kwargs['params']}")

    print_kwargs()


dag_object = demo_notifier()


if __name__ == "__main__":
    dag_object.test()
