"""Dag is a demo designed to test pg connection."""
import pendulum
import pandas as pd
from airflow.decorators import dag, task, setup
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    "demo_postgres_connection",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["demo"]
)
def demo_postgres_connection():
    """
    Test pg connection
    """
    def _fix_columns(df: pd.DataFrame, cols_mapping: dict):
        if cols_mapping is not None:
            df.columns = [cols_mapping[col] if col in cols_mapping else col for col in df.columns.tolist()]

        now = pendulum.now("UTC").to_datetime_string()
        df["create_time"] = now
        df["update_time"] = now
        return df

    @task()
    def extract_history(file_name: str) -> pd.DataFrame:
        from integration.source.file import FileSource
        file_source = FileSource()
        return file_source.read_csv(file_name, "file")

    @task()
    def write_using_insert(pg_hook: PostgresHook, df: pd.DataFrame, table_schema: str, table_name: str):

        from contextlib import closing
        from psycopg2.extras import execute_values

        df = _fix_columns(df, None)
        table_columns = ",".join(df.columns)
        sql_statement = f"INSERT INTO {table_schema}.{table_name} ({table_columns}) VALUES %s RETURNING id;"

        with closing(pg_hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            execute_values(cur, sql_statement, df.values.tolist())
            result = cur.fetchone()[0]
            conn.commit()

        return result

    @task()
    def write_using_copy(pg_hook: PostgresHook, df: pd.DataFrame, table_schema: str, table_name: str):

        import tempfile

        with tempfile.NamedTemporaryFile() as temp_file:

            cols_mapping = {
                "adjClose": "adjust_close",
                "unadjustedVolume": "unadjusted_volume",
                "changePercent": "change_percent",
                "changeOverTime": "change_over_time",
            }

            df = _fix_columns(df, cols_mapping)
            table_columns = ",".join(df.columns)
            sql_statement = f"COPY {table_schema}.{table_name} ({table_columns}) FROM STDIN WITH CSV HEADER;"

            df.to_csv(temp_file.name, index=False)
            pg_hook.copy_expert(sql_statement, temp_file.name)

    @task()
    def read_using_sql(pg_hook: PostgresHook, sql: str, sql_params: dict | None = None) -> pd.DataFrame:
        engine = pg_hook.get_sqlalchemy_engine()
        return pd.read_sql(sql, con=engine, params=sql_params)

    @task()
    def read_using_sql_file(pg_hook: PostgresHook, sql: str, sql_params: dict | None = None) -> pd.DataFrame:
        if sql.endswith(".sql"):
            with open(sql, "r", encoding="utf-8") as file:
                sql = file.read()
        engine = pg_hook.get_sqlalchemy_engine()
        return pd.read_sql(sql, con=engine, params=sql_params)

    @setup
    def truncate_table(pg_hook: PostgresHook, table_schema: str, table_name: str) -> str:
        from contextlib import closing

        sql_statement = f"TRUNCATE TABLE {table_schema}.{table_name};"

        with closing(pg_hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.execute(sql_statement)
            conn.commit()
        return "truncate success"

    pg_hook = PostgresHook(postgres_conn_id="pg_test")
    table_name = "fmp_stock_price"
    raw_table_name = f"_raw_{table_name}"

    # test read
    read_using_sql(pg_hook, "SELECT * FROM dim.dim_date WHERE dt = %(dt)s", {"dt": "2024-01-01"})
    read_using_sql_file(pg_hook, "/opt/airflow/include/sql/test.sql", {"symbol": "TQQQ", "week_start": "2024-01-01"})

    # test insert
    raw_df = pd.DataFrame.from_dict({"metadata": ['{"a": 1, "b": "2"}']})
    write_using_insert(pg_hook, raw_df, "stock", raw_table_name)

    # test copy
    historic_df = extract_history("fmp_historic_data.csv")
    truncate_table(pg_hook, "stock", table_name) >> write_using_copy(pg_hook, historic_df, "stock", table_name)


dag_object = demo_postgres_connection()


if __name__ == "__main__":
    dag_object.test()
