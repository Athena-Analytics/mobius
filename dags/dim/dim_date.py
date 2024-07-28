"""Dag builds dim_date."""

import logging

import pandas as pd
import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.models.param import Param

from integration.destination.postgres import PGDestination

logger = logging.getLogger(__name__)


@dag(
    "dim_date",
    default_args={"depends_on_past": False},
    description="build dim_date table",
    start_date=pendulum.today(),
    schedule=None,
    catchup=False,
    tags=["dim"],
    params={
        "env": Param(
            "prod", type="string", title="Select one Value", enum=["prod", "dev"]
        ),
        "sync_mode": Param(
            "Incremental Append",
            type="string",
            title="Select on Value.",
            enum=["Incremental Append", "Full Refresh Append"],
        ),
        "start": Param(
            f"{pendulum.today().date()}",
            type="string",
            format="date",
            title="Start Date Picker",
        ),
        "end": Param(
            f"{pendulum.today().date()}",
            type="string",
            format="date",
            title="End Date Picker",
        ),
    },
)
def dim_date():
    """
    Build a dim table of date that has columns
    - id
    - dt
    - date
    - year_num
    - year_start
    - year_end
    - quarter_num
    - quarter_start
    - quarter_end
    - month_num
    - month_start
    - month_end
    - week_num
    - natural_week_start
    - natural_week_end
    - natural_week_range
    - foreign_natural_week_start
    - foreign_natural_week_end
    - foreign_natural_week_range
    - work_week_start
    - work_week_end
    - work_week_range
    - frequency_of_month
    - create_time
    - update_time
    """

    def _get_year_properties(d: pendulum.datetime) -> dict:
        return dict(
            year_num=d.year,
            year_start=d.start_of("year").to_datetime_string(),
            year_end=d.end_of("year").to_datetime_string(),
        )

    def _get_quarter_properties(d: pendulum.datetime) -> dict:
        return dict(
            quarter_num=d.quarter,
            quarter_start=d.first_of("quarter").to_datetime_string(),
            quarter_end=d.last_of("quarter").end_of("day").to_datetime_string(),
        )

    def _get_month_properties(d: pendulum.datetime) -> dict:
        return dict(
            month_num=d.month,
            month_start=d.start_of("month").to_datetime_string(),
            month_end=d.end_of("month").to_datetime_string(),
        )

    def _get_week_properties(d: pendulum.datetime) -> dict:
        week_start = d.start_of("week")
        week_end = d.end_of("week")

        foreign_natural_week_start = week_start.subtract(days=1)
        foreign_natural_week_end = week_end.subtract(days=1)

        work_week_start = week_start.subtract(days=2)
        work_week_end = week_end.subtract(days=2)

        return dict(
            week_num=d.day_of_week + 1,
            natural_week_start=week_start.to_datetime_string(),
            natural_week_end=week_end.to_datetime_string(),
            natural_week_range=week_start.to_date_string() + "~" + week_end.to_date_string(),
            foreign_natural_week_start=foreign_natural_week_start.to_datetime_string(),
            foreign_natural_week_end=foreign_natural_week_end.to_datetime_string(),
            foreign_natural_week_range=foreign_natural_week_start.to_date_string() + "~" + foreign_natural_week_end.to_date_string(),
            work_week_start=work_week_start.to_datetime_string(),
            work_week_end=work_week_end.to_datetime_string(),
            work_week_range=work_week_start.to_date_string() + "~" + work_week_end.to_date_string(),
        )

    def single_date_info(d: pendulum.datetime) -> pd.DataFrame:
        y = _get_year_properties(d)
        q = _get_quarter_properties(d)
        m = _get_month_properties(d)
        w = _get_week_properties(d)
        yq = y | q
        yqm = yq | m
        yqmw = yqm | w
        yqmw["dt"] = d.to_date_string()
        yqmw["date"] = d.format("YYYY-MM-DD")
        result = pd.DataFrame([yqmw])
        return result

    def frequency_of_month(dt: pendulum.datetime) -> pd.DataFrame:
        import calendar

        cal = calendar.Calendar()
        month_days = list(cal.itermonthdays4(dt.year, dt.month))

        lst = []
        for i in range(0, 7):
            some_month_days = [
                month_days[i + j * 7] for j in range(len(month_days) // 7)
            ]
            some_week_day = [
                pendulum.date(k[0], k[1], k[2]).to_date_string()
                for k in some_month_days
                if k[1] == dt.month
            ]
            df = pd.DataFrame(some_week_day, columns=["dt"]).reset_index()
            df["frequency_of_month"] = df["index"] + 1
            lst.append(df)
        result = pd.concat(lst)
        return result

    @task.branch()
    def branch_sync(**kwargs) -> str:
        params = kwargs["params"]
        sync_mode = params["sync_mode"]

        if sync_mode == "Incremental Append":
            return "current_task_group.extract"
        elif sync_mode == "Full Refresh Append":
            return "historic_task_group.extract_history"
        else:
            raise AirflowException(
                "sync_mode only support Incremental Append and Full Refresh Append"
            )

    @task()
    def extract_history(file: str) -> pd.DataFrame:
        from airflow.hooks.filesystem import FSHook

        fs_hook = FSHook()
        path = fs_hook.get_path()
        return pd.read_csv(f"{path}/file/{file}")

    @task()
    def extract(**kwargs) -> pd.DataFrame:
        params = kwargs["params"]
        try:
            if "start" in params and "end" in params:
                start = pendulum.parse(params["start"])
                end = pendulum.parse(params["end"])
                interval = pendulum.interval(start, end)
                single_date_result = pd.concat(
                    [single_date_info(i) for i in interval.range("days")]
                )
                week_times_result = pd.concat(
                    [frequency_of_month(j) for j in interval.range("months")]
                )
                result = single_date_result.merge(
                    week_times_result[["frequency_of_month", "dt"]], on="dt", how="left"
                )
                return result
            else:
                raise ValueError(
                    f"both of start and end must be existent, but got {start} and {end}"
                )
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    @task()
    def load(df: pd.DataFrame, table_name: str, **kwargs):
        params = kwargs["params"]
        env = params["env"]

        try:
            if df is None:
                raise TypeError("df is None")

            logger.info("dag running on env: %s", env)

            pg_destination = PGDestination(env)
            result = pg_destination.copy_write(df, table_name, "dim")

            return result
        except Exception as e:
            raise AirflowException(f"unknown error: {e}") from e

    branch_sync_op = branch_sync()

    @task_group()
    def current_task_group():
        current_df = extract()
        load(current_df, "dim_date")

    @task_group()
    def historic_task_group():
        historic_df = extract_history("dim_date.csv")
        load(historic_df, "dim_date")

    current_task = current_task_group()
    historic_task = historic_task_group()

    branch_sync_op >> [current_task, historic_task]


dag_object = dim_date()


if __name__ == "__main__":
    conf = {
        "env": "dev",
        "sync_mode": "Full Refresh Append",
        "start": "2999-01-01",
        "end": "2999-01-01",
    }
    dag_object.test(run_conf=conf)
