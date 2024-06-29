"""Dag is a demo designed to test some params."""
import os
from pprint import pprint

import pendulum
from airflow.decorators import dag, task


@dag(
    "demo_param",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["demo"],
    params={
        "env": "dev",
        "conf1": "1",
        "conf2": "2"
    }
)
def demo_param():
    """
    Test some conditions using param that is
    - dag_run
    - kwargs
    - test_mode
    """
    @task()
    def print_kwargs(**kwargs):
        pprint(kwargs)
        print(f"params of kwargs is {kwargs['params']}")

    @task()
    def print_task_param(test_mode=None):
        """
        Print out the "foo" param passed in via
        `airflow tasks test example_passing_params_via_test_command env_var_test_task <date>
        --env-vars '{"foo":"bar"}'`
        """
        print(f"test_mode is {test_mode}")
        if test_mode:
            print(f"env is {os.environ.get('env')}")
            print(f"AIRFLOW_TEST_MODE is {os.environ.get('AIRFLOW_TEST_MODE')}")

    print_kwargs()
    print_task_param()


dag_object = demo_param()


if __name__ == "__main__":
    dag_object.test()
