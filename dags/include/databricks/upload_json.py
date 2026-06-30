from __future__ import annotations

import json
import logging
import re
from urllib.parse import quote

import requests

try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook

from include.config.constant import (
    databricks_conn_id,
    databricks_volume_weather_bronze_path,
)


def upload_json_to_databricks_volume(
    batch_date: str,
    object_name: str,
    country: str,
    data: str,
) -> str:
    target_path = _build_volume_file_path(
        batch_date=batch_date,
        object_name=object_name,
        country=country,
    )

    try:
        host, token = _get_databricks_credentials()
        _create_volume_directory(
            host=host,
            token=token,
            directory_path=target_path.rsplit("/", 1)[0],
        )
        _upload_file(
            host=host,
            token=token,
            file_path=target_path,
            data=_serialize_json_payload(data),
        )
        logging.info("Uploaded weather bronze JSON to Databricks: %s", target_path)
    except Exception as exc:
        logging.warning(
            "Failed to upload weather bronze JSON to Databricks path %s: %s",
            target_path,
            exc,
        )

    return target_path


def _build_volume_file_path(batch_date: str, object_name: str, country: str) -> str:
    safe_country = _sanitize_path_value(country)

    return (
        f"{databricks_volume_weather_bronze_path}"
        f"/date={batch_date}"
        f"/{object_name}"
        f"/country={safe_country}.json"
    )


def _sanitize_path_value(value: str) -> str:
    normalized = value.strip().replace(" ", "_")
    sanitized = re.sub(r"[^A-Za-z0-9_.=-]", "_", normalized)
    return sanitized or "unknown"


def _get_databricks_credentials() -> tuple[str, str]:
    conn = BaseHook.get_connection(databricks_conn_id)
    host = (conn.host or "").rstrip("/")
    token = conn.password

    if not token and getattr(conn, "extra_dejson", None):
        token = conn.extra_dejson.get("token")

    if not host:
        raise ValueError(f"Airflow connection {databricks_conn_id} has no host")

    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"

    if not token:
        raise ValueError(f"Airflow connection {databricks_conn_id} has no token")

    return host, token


def _create_volume_directory(host: str, token: str, directory_path: str) -> None:
    response = requests.put(
        _build_files_api_url(host, "directories", directory_path),
        headers=_build_auth_headers(token),
        timeout=30,
    )
    response.raise_for_status()


def _upload_file(host: str, token: str, file_path: str, data: bytes) -> None:
    response = requests.put(
        f"{_build_files_api_url(host, 'files', file_path)}?overwrite=true",
        headers={
            **_build_auth_headers(token),
            "Content-Type": "application/octet-stream",
        },
        data=data,
        timeout=60,
    )
    response.raise_for_status()


def _build_files_api_url(host: str, resource: str, path: str) -> str:
    encoded_path = quote(path, safe="/")
    return f"{host}/api/2.0/fs/{resource}{encoded_path}"


def _build_auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _serialize_json_payload(data: str) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")

    return json.dumps(data, ensure_ascii=False).encode("utf-8")
