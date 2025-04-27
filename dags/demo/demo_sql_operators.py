"""Dag is a demo designed to test executing sql file."""

import pendulum
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    "demo_sql_operators",
    start_date=pendulum.today(),
    schedule=None,
    template_searchpath="/opt/airflow/include",
    catchup=False,
    tags=["demo"],
)
def demo_sql_operators():
    """
    Test sql operators
    """
    SQLExecuteQueryOperator(
        conn_id="pg_test",
        task_id="read_using_sql_file",
        sql="sql/sql_parameter.sql",
        parameters={"symbol": "TQQQ", "week_start": "2024-01-01"},
        return_last=False,
    )


dag_object = demo_sql_operators()


if __name__ == "__main__":
    dag_object.test()
