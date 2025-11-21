# parquet_reader.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - fail fast if pyarrow missing
    raise ImportError(
        "ParquetReader requires 'pyarrow'. Install with `pip install pyarrow`."
    ) from exc


PathLike = Union[str, Path]


class ParquetReader:
    """
    Utility for reading parquet data from local or networked storage.

    Parameters
    ----------
    base_path : str | Path | None
        Optional root directory to resolve relative parquet file paths against.
        If None, relative paths are interpreted relative to the current working directory.
    engine : {{'pyarrow', 'fastparquet'}}
        Engine to hand over to pandas for reading parquet files when filter pushdown
        is not required. Defaults to 'pyarrow'.
    """

    SUPPORTED_ENGINES = {"pyarrow", "fastparquet"}

    def __init__(self, base_path: Optional[PathLike] = None, engine: str = "pyarrow") -> None:
        if engine not in self.SUPPORTED_ENGINES:
            raise ValueError(f"Unsupported engine '{engine}'. Choose from: {self.SUPPORTED_ENGINES}")

        self.base_path = Path(base_path).expanduser().resolve() if base_path else None
        self.engine = engine

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def load(
        self,
        source: PathLike,
        *,
        columns: Optional[Iterable[str]] = None,
        filters: Optional[list] = None,
        to_pandas_kwargs: Optional[dict] = None,
        **read_kwargs,
    ) -> pd.DataFrame:
        """
        Read parquet data into a pandas DataFrame.

        Parameters
        ----------
        source : str | Path
            File path, directory path, or URL understood by pandas/pyarrow.
            Networked paths (e.g. s3://, gs://) require fsspec-compatible dependencies.
        columns : Iterable[str], optional
            Subset of columns to read.
        filters : list, optional
            Row filters (aka predicate pushdown). Only supported when using pyarrow.
            Format follows pyarrow's filtering syntax:
            https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html
        to_pandas_kwargs : dict, optional
            Extra kwargs passed to `Table.to_pandas()` when using pyarrow with filters.
        **read_kwargs :
            Additional keyword arguments forwarded to the underlying reader.

        Returns
        -------
        pd.DataFrame
        """
        path_or_url = self._resolve_path(source)

        if filters and self.engine != "pyarrow":
            raise ValueError("Row filters are only supported with the 'pyarrow' engine.")

        if filters:
            table = pq.read_table(path_or_url, columns=columns, filters=filters, **read_kwargs)
            to_pandas_kwargs = to_pandas_kwargs or {}
            return table.to_pandas(**to_pandas_kwargs)

        return pd.read_parquet(
            path_or_url,
            columns=columns,
            engine=self.engine,
            **read_kwargs,
        )

    def query(
        self,
        source: PathLike,
        *,
        expr: str,
        columns: Optional[Iterable[str]] = None,
        filters: Optional[list] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load a parquet dataset and apply a pandas-style `DataFrame.query`.

        Parameters
        ----------
        expr : str
            Query expression understood by pandas. Example: "country == 'US' and price > 100".
        Other parameters mirror `load`.

        Returns
        -------
        pd.DataFrame
        """
        df = self.load(source, columns=columns, filters=filters, **kwargs)
        return df.query(expr)

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #
    def _resolve_path(self, source: PathLike) -> Union[str, Path]:
        source = Path(source) if isinstance(source, Path) else Path(str(source))

        # Allow remote URLs (s3:// etc.) by returning the original string.
        if source.drive or source.root or str(source).startswith(tuple(["s3://", "gs://", "http://", "https://"])):
            return str(source)

        if self.base_path:
            return (self.base_path / source).resolve()

        return source.resolve()

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        base = str(self.base_path) if self.base_path else "."
        return f"{self.__class__.__name__}(base_path='{base}', engine='{self.engine}')"