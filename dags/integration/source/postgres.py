"""Module is Source of PostgreSQL."""
import logging

from pandas import DataFrame
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
            self.pg_hook = PostgresHook(postgres_conn_id="pg_test")
        elif env == "prod":
            self.pg_hook = PostgresHook(postgres_conn_id="pg_prod")
        else:
            raise ValueError(f"env must be dev or prod, but got {env}")

    def exist(self, table_name: str, table_schema: str = None) -> bool:
        """
        Define if a table exists in PostgreSQL
        """
        if table_schema is None:
            stmt = f"SELECT COUNT(1) FROM information_schema.columns WHERE table_name = {table_name}"
        else:
            stmt += f" AND table_schema = {table_schema}"

        result = self.read(stmt)

        if result.size > 0:
            return True
        else:
            return False

    def read(self, sql: str, **kwargs) -> DataFrame:
        """
        Define how to fetch data from PostgreSQL using SQL
        """
        try:
            from pandas.io import sql as psql
            engine = self.pg_hook.get_sqlalchemy_engine()
            return psql.read_sql(sql, con=engine, **kwargs)
        except Exception as e:
            raise e
