
from datetime import datetime
from include.storage.postgres_storage import PostgresStorage

postgres_storage = PostgresStorage()

def get_pipeline_state(pipeline_name: str) -> dict | None:

    sql = """
    SELECT status, last_processed_at, row_count
      FROM public.etl_pipeline_state
     WHERE pipeline_name = %s
    """

    result = postgres_storage.get_first(
        sql,
        (pipeline_name,)
    )

    if result is None:
        return None

    status, last_processed_at, row_count = result

    return {
        'status': status,
        'last_processed_at': last_processed_at,
        'row_count': row_count
    }

def update_pipeline_state(
    pipeline_name: str, 
    status: str,
    row_count: int | None = None,
    last_processed_at = None
    ) -> None:

    sql = """
    UPDATE public.etl_pipeline_state
    SET status = %s,
        row_count = COALESCE(%s, row_count),
        last_processed_at = COALESCE(%s, last_processed_at),
        last_run_at = CASE 
                            WHEN %s = 'RUNNING'
                            THEN CURRENT_TIMESTAMP
                            ELSE last_run_at
                        END,
            updated_at = CURRENT_TIMESTAMP 
    WHERE pipeline_name = %s
    """
    postgres_storage.execute(
        sql, 
        (
            status,
            row_count,
            last_processed_at,
            status,
            pipeline_name,
        )
    )

def get_row_count(
    table_name: str,
    batch_datetime: str
) -> int:
    
    valid_tables = {
        "staging_forecast",
        "staging_alert"
    }

    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}")

    sql = f"""
    SELECT count(*)
      FROM public.{table_name}
     WHERE batch_time = %s
    """

    result = postgres_storage.get_first(
        sql,
        (batch_datetime,)
    )

    if result is None:
        return 0

    return result[0]

if __name__ == "__main__":
    print(get_pipeline_state("weather_data_pipeline"))