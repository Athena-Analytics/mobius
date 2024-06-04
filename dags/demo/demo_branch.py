"""Dag is a demo designed to test branch."""
import logging
import textwrap

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from utils.common_utils import get_task_date

logger = logging.getLogger(__name__)


@dag(
    "demo_branch",
    start_date=pendulum.today(),
    schedule="@once",
    tags=["demo"]
)
def demo_branch():
    """
    Test branch that is
    - t1
    - t2
    """
    @task.branch()
    def branch_task(**kwargs) -> str:

        start_time = kwargs["data_interval_start"]
        logger.info("data_interval_start is %s", start_time)

        if start_time.day_of_week == 1:
            return "t1"
        else:
            return "t2"

    task_date_op = get_task_date()

    branch_op = branch_task()

    t1_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    t1 = BashOperator(task_id="t1", bash_command=t1_command)

    t2_command = textwrap.dedent(
        """
        echo "{{ data_interval_start  }}" "{{ data_interval_end }}"
        """
    )
    t2 = BashOperator(task_id="t2", bash_command=t2_command)

    task_date_op >> branch_op >> [t1, t2]


dag_object = demo_branch()


if __name__ == "__main__":
    dag_object.test()
