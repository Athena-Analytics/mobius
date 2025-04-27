"""Module is Destination of PostgreSQL."""

import logging
from contextlib import closing

import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from integration.destination.base import BaseDestination

logger = logging.getLogger(__name__)


class PGDestination(BaseDestination):
    """
    Define how to write data into PostgreSQL
    """

    def __init__(self, env: str):

        logger.info("current env of postgresql is %s", env)

        if env == "dev":
            self.conn_id = "pg_test"
            self._pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        elif env == "prod":
            self.conn_id = "pg_prod"
            self._pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        else:
            raise ValueError(f"env must be dev or prod, but got {env}")

    @staticmethod
    def _fix_columns(df: pd.DataFrame, cols_mapping: dict) -> pd.DataFrame:
        if cols_mapping is not None:
            df.columns = [
                cols_mapping[col] if col in cols_mapping else col
                for col in df.columns.tolist()
            ]
            logger.info("transfer columns successfully")

        now = pendulum.now("UTC").to_datetime_string()
        df["create_time"] = now
        df["update_time"] = now
        return df

    def _execute(self, s: str, params: dict | None = None) -> int | None:
        """
        Execute sql
        """
        import os

        try:
            if self.conn_id != "pg_test":
                raise ValueError(f"conn_id must be pg_test, but got {self.conn_id}")

            if os.path.isfile(s):
                with open(s, "r", encoding="utf-8") as file:
                    sql = file.read()
            else:
                sql = s

            with closing(self._pg_hook.get_conn()) as conn, closing(
                conn.cursor()
            ) as cur:
                cur.execute(sql, params)
                conn.commit()

            return 1
        except AirflowException as e:
            logger.error(e)

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_schema: str = None,
        cols_mapping: dict = None,
    ) -> int | None:
        """
        Insert data using INSERT command
        """
        from psycopg2.extras import execute_values

        try:
            df = self._fix_columns(df, cols_mapping)
            table_columns = ",".join(df.columns)
            sql_statement = f"INSERT INTO {table_schema}.{table_name} ({table_columns}) VALUES %s RETURNING id;"

            with closing(self._pg_hook.get_conn()) as conn, closing(
                conn.cursor()
            ) as cur:
                execute_values(cur, sql_statement, df.values.tolist())
                result = cur.fetchone()[0]
                conn.commit()

            return result
        except AirflowException as e:
            logger.error(e)

    def copy_write(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_schema: str = None,
        cols_mapping: dict = None,
    ) -> int | None:
        """
        Insert data using COPY command
        """
        import tempfile

        try:
            with tempfile.NamedTemporaryFile() as temp_file:
                df = self._fix_columns(df, cols_mapping)
                table_columns = ",".join(df.columns)
                sql_statement = f"COPY {table_schema}.{table_name} ({table_columns}) FROM STDIN WITH CSV HEADER;"

                temp_file_name = temp_file.name
                logger.info("name of tempfile is %s", temp_file_name)

                df.to_csv(temp_file_name, index=False)
                self._pg_hook.copy_expert(sql_statement, temp_file_name)

            return 1
        except AirflowException as e:
            logger.error(e)
