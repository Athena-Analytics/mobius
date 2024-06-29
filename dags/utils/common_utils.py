"""These are some common task."""
from datetime import timedelta

from airflow.decorators import task


@task()
def get_task_date(reduce_days_for_start: int = 0,
                  reduce_days_for_end: int = 0,
                  **kwargs) -> dict[str, str]:
    """
    Get date of scheduler while task is runing
    """
    start_date = kwargs["data_interval_start"] - timedelta(days=reduce_days_for_start)
    end_date = kwargs["data_interval_end"] - timedelta(days=reduce_days_for_end)

    if start_date > end_date :
        raise ValueError(f"start_date must less than or equal with end_date, but got {start_date}:{end_date}")

    return {"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d")}
