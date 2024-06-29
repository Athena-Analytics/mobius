"""Dag is a demo designed to test pg connection."""
import pendulum
import pandas as pd
from airflow.decorators import dag, task, setup
from airflow.hooks.filesystem import FSHook
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
    def extract_history(path: str, file: str) -> pd.DataFrame:
        df = pd.read_csv(f"{path}/file/{file}")
        if "t" in df.columns.tolist():
            df["t"] = df["t"].apply(lambda x: pendulum.parse(x).int_timestamp * 1000)
        return df

    @task()
    def insert_to_postgres(pg_hook: PostgresHook, df: pd.DataFrame, table_schema: str, table_name: str):

        from contextlib import closing
        from psycopg2.extras import execute_values

        df = _fix_columns(df, None)
        table_columns = ",".join(df.columns)
        sql_statment = f"INSERT INTO {table_schema}.{table_name} ({table_columns}) VALUES %s RETURNING id;"

        with closing(pg_hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            execute_values(cur, sql_statment, df.values.tolist())
            result = cur.fetchone()[0]
            conn.commit()

        return result

    @task()
    def save_to_postgres(pg_hook: PostgresHook, df: pd.DataFrame, table_schema: str, table_name: str):

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
    def read_from_postgres(pg_hook: PostgresHook, sql: str) -> pd.DataFrame:
        from pandas.io import sql as psql
        engine = pg_hook.get_sqlalchemy_engine()
        df = psql.read_sql(sql, con=engine)
        return df

    @setup
    def truncate_table(pg_hook: PostgresHook, table_schema: str, table_name: str):
        from contextlib import closing

        sql_statment = f"TRUNCATE TABLE {table_schema}.{table_name};"

        with closing(pg_hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.execute(sql_statment)
            conn.commit()
        return "truncate success"

    pg_hook = PostgresHook(postgres_conn_id="pg_test")
    fs_hook = FSHook()
    path = fs_hook.get_path()
    table_name = "fmp_stock_aggregates_bars"
    raw_table_name = f"_raw_{table_name}"

    # test read
    read_from_postgres(pg_hook, "SELECT * FROM dim.dim_date")

    # test insert
    raw_df = pd.DataFrame.from_dict({"metadata": ['{"a": 1, "b": "2"}']})
    insert_to_postgres(pg_hook, raw_df, "stock", raw_table_name)

    # test copy
    historic_df = extract_history(path, "fmp_tqqq_historic_data.csv")
    truncate_table(pg_hook, "stock", table_name) >> save_to_postgres(pg_hook, historic_df, "stock", table_name)


dag_object = demo_postgres_connection()


if __name__ == "__main__":
    dag_object.test()
