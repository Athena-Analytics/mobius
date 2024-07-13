"""Module is Source of PostgreSQL."""
import logging

from pandas import DataFrame, read_sql
from airflow.providers.postgres.hooks.postgres import PostgresHook

from integration.source.base import BaseSource

logger = logging.getLogger(__name__)


class PGSource(BaseSource):
    """
    Define how to read data from PostgreSQL
    """
    def __init__(self, env: str):

        logger.info("current env of postgresql is %s", env)

        if env == "dev":
            self._pg_hook = PostgresHook(postgres_conn_id="pg_test")
        elif env == "prod":
            self._pg_hook = PostgresHook(postgres_conn_id="pg_prod")
        else:
            raise ValueError(f"env must be dev or prod, but got {env}")

    def exist(self, table_name: str, table_schema: str = None) -> bool:
        """
        Check if a table exists
        """
        if table_schema is None:
            stmt = f"SELECT COUNT(1) FROM information_schema.columns WHERE table_name = {table_name}"
        else:
            stmt += f" AND table_schema = {table_schema}"

        result = self.read(stmt)

        if result.size > 0:
            return True
        return False

    def read(self, sql: str, sql_params: dict | None = None) -> DataFrame:
        """
        Fetch data using SQL
        """
        try:
            if sql.endswith(".sql"):
                with open(sql, "r", encoding="utf-8") as file:
                    sql_statement = file.read()
            else:
                sql_statement = sql
            engine = self._pg_hook.get_sqlalchemy_engine()
            logger.info("begin executing %s", sql_statement)
            return read_sql(sql_statement, con=engine, params=sql_params)
        except Exception as e:
            raise e
