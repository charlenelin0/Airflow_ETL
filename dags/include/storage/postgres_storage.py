
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.config.constant import postgres_conn_id
from sqlalchemy import Engine
from typing import Any

class PostgresStorage:

    def __init__(self):
        self._hook = PostgresHook(
            postgres_conn_id = postgres_conn_id
        )

    def get_sqlalchemy_engine(self) -> Engine:
        return self._hook.get_sqlalchemy_engine()

    def execute(
        self,
        sql: str,
        parameters: tuple | None = None
    ) -> None:
        self._hook.run(
            sql,
            parameters = parameters
        )

    def get_first(
        self,
        sql: str,
        parameters: tuple | None = None
    ) -> tuple[Any, ...] | None:
        return self._hook.get_first(
            sql, 
            parameters = parameters
        )

    def get_records(
        self,
        sql: str,
        parameters: tuple | None = None
    ) -> list[tuple[Any, ...]]:
        return self._hook.get_records(
            sql,
            parameters = parameters
        )