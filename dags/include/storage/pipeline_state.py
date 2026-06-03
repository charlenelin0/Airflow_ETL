
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_pipeline_state(pipeline_name: str) -> dict | None:

    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')

    result = hook.get_first("""
    SELECT status, last_processed_at, row_count
      FROM public.etl_pipeline_state
     WHERE pipeline_name = %s
    """,
    parameters = (pipeline_name,)
    )

    if result is None:
        return None

    return {
        'status': result[0],
        'last_processed_at': result[1],
        'row_count': result[2]
    }

def update_pipeline_state(
    pipeline_name: str, 
    status: str,
    row_count: int | None = None,
    last_processed_at = None
    ) -> None:

    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

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

    cursor.execute(
        sql, 
        (
            status,
            row_count,
            last_processed_at,
            status,
            pipeline_name,
        )
    )

    conn.commit()

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

    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')

    result = hook.get_first(f"""
    SELECT count(*)
      FROM public.{table_name}
     WHERE batch_time = %s
    """,
    parameters = (batch_datetime,)
    )

    return result[0]

if __name__ == "__main__":
    print(get_pipeline_state("weather_data_pipeline"))