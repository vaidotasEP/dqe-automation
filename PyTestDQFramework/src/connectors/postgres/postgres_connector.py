from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as PsycopgConnection


class PostgresConnectorContextManager:
    """
    Context manager that opens a psycopg2 connection and exposes helper APIs.
    """

    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        db_port: int = 5432,
        connect_timeout: int = 10,
        autocommit: bool = True,
        cursor_factory=RealDictCursor,
    ) -> None:
        self._connection_params: Dict[str, Any] = {
            "host": db_host,
            "dbname": db_name,
            "user": db_user,
            "password": db_password,
            "port": db_port,
            "connect_timeout": connect_timeout,
            "cursor_factory": cursor_factory,
        }
        self._autocommit = autocommit
        self._connection: Optional[PsycopgConnection] = None

    def __enter__(self) -> "PostgresConnectorContextManager":
        try:
            self._connection = psycopg2.connect(**self._connection_params)
            self._connection.autocommit = self._autocommit
            return self
        except psycopg2.Error as exc:  # pragma: no cover - passthrough for clarity
            raise RuntimeError("Unable to establish PostgreSQL connection") from exc

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        if self._connection is None:
            return None

        if exc_type is not None and not self._autocommit:
            # Rollback failed transaction so the connection can close cleanly.
            self._connection.rollback()

        self._connection.close()
        self._connection = None
        return None  # never suppress exceptions

    @property
    def connection(self) -> PsycopgConnection:
        if self._connection is None:
            raise RuntimeError("Connection has not been established. Use 'with' first.")
        return self._connection

    def get_data_sql(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute *query* and return its rows as a DataFrame."""

        if not isinstance(query, str) or not query.strip():
            raise ValueError("SQL query must be a non-empty string")

        return pd.read_sql_query(query, self.connection, params=params)
