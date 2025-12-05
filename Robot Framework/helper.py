from __future__ import annotations

import re
import time
import warnings
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

import pandas as pd
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from robot.api.deco import keyword
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By

PathLike = Union[str, Path]


class HelperLibrary:
    # ------------------------------------------------------------------ #
    # Public keywords
    # ------------------------------------------------------------------ #

    @keyword
    def read_html_table_to_pandas(self, table_element) -> pd.DataFrame:
        """Reading HTML table data into a Pandas DataFrame. Scrolling was added to make sure the entire table gets read"""
        driver = table_element.parent

        if table_element.get_attribute("class") != "table":
            table_element = table_element.find_element(By.CSS_SELECTOR, "g.table")

        scroll_target = table_element.find_element(
            By.CSS_SELECTOR, "g.table-control-view"
        )

        all_rows: dict[Decimal, dict[str, str]] = {}
        previous_signature: tuple[str, ...] | None = None

        while True:
            for key, row in self._read_visible_rows(table_element).items():
                all_rows.setdefault(key, {}).update(row)

            signature = tuple(
                block.get_attribute("transform")
                for block in table_element.find_elements(
                    By.CSS_SELECTOR,
                    "g.y-column g.column-block[id^='cells']",
                )
            )
            if signature == previous_signature:
                break

            driver.execute_script(
                "arguments[0].dispatchEvent("
                "new WheelEvent('wheel', {deltaY: arguments[1], bubbles: true}));",
                scroll_target,
                scroll_target.size["height"],
            )
            previous_signature = signature
            time.sleep(0.2)

        if not all_rows:
            return pd.DataFrame()

        rows = [all_rows[key] for key in sorted(all_rows)]
        df = pd.DataFrame(rows)
        return self._standardize_dataframe(df)

    @keyword
    def read_parquet_to_pandas(
        self,
        path: PathLike,
        columns: Optional[Union[str, Sequence[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        date_column: str = "visit_date",
    ) -> pd.DataFrame:
        """Reading partitioned Parquet dataset with date filtering into a Pandas DataFrame. We do some pre-processing to match it with the HTML table we will use for comparison"""
        columns = self._normalize_column_names(columns)
        filters = self._build_date_filters(date_column, start_date, end_date)

        df = self._load_parquet(path, columns=columns, filters=filters)

        df = df.rename(
            columns={
                "avg_time_spent": "average_time_spent",
            }
        )
        return self._standardize_dataframe(df)

    @keyword
    def compare_dataframes(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        key_columns: Optional[Union[str, Sequence[str]]] = None,
    ) -> pd.DataFrame:
        """
        Comparing two DataFrames for exact match and returning differences.
            Pass the test if they match.
            Fail the test if mismatches are found, showing the differences.
        """
        left = self._standardize_dataframe(df1)
        right = self._standardize_dataframe(df2)

        keys = self._normalize_column_names(key_columns)
        if keys:
            missing_left = [col for col in keys if col not in left.columns]
            missing_right = [col for col in keys if col not in right.columns]
            if missing_left or missing_right:
                raise KeyError(
                    f"Key columns missing in actual: {missing_left!r}, expected: {missing_right!r}"
                )
            left = left.set_index(keys).sort_index()
            right = right.set_index(keys).sort_index()
        else:
            left = left.sort_index()
            right = right.sort_index()

        left = left.sort_index(axis=1).convert_dtypes()
        right = right.sort_index(axis=1).convert_dtypes()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            aligned_left, aligned_right = left.align(right, join="outer", axis=0)

        try:
            pd.testing.assert_frame_equal(
                aligned_left,
                aligned_right,
                check_dtype=False,
                check_like=True,
                check_exact=True,
            )
            return pd.DataFrame()
        except AssertionError:
            diff = aligned_left.compare(
                aligned_right,
                align_axis=0,
                keep_shape=False,
                keep_equal=False,
            ).dropna(axis=1, how="all")
            with pd.option_context(
                "display.max_rows", None,
                "display.max_columns", None,
                "display.width", None,
            ):
                raise AssertionError(
                    "DataFrames do not match. Differences (actual vs expected):\n"
                    f"{diff.to_string()}"
                )

    # ------------------------------------------------------------------ #
    # Normalization helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Bring column names, dates, and numeric fields to a consistent schema."""

        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
        )

        if "average_time_spent" in df.columns:
            df["average_time_spent"] = pd.to_numeric(
                df["average_time_spent"], errors="coerce"
            ).round(2)

        for column in df.columns:
            if "date" in column:
                try:
                    df[column] = pd.to_datetime(df[column]).dt.normalize()
                except (ValueError, TypeError):
                    pass

        return df

    @staticmethod
    def _normalize_column_names(
        columns: Optional[Union[str, Sequence[str]]],
    ) -> Optional[list[str]]:
        if columns is None:
            return None
        if isinstance(columns, str):
            columns = [chunk.strip() for chunk in columns.split(",")]
        return [str(name).strip() for name in columns if str(name).strip()]


    # ------------------------------------------------------------------ #
    # Parquet helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_date_filters(
        date_column: str, start_date: Optional[str], end_date: Optional[str]
    ) -> Optional[list[list[tuple[str, str, Any]]]]:
        def _parse(value: Optional[str]):
            if value is None:
                return None
            ts = pd.Timestamp(value)
            if ts.tzinfo is not None:
                ts = ts.tz_convert(None)
            return ts.to_pydatetime()

        conditions: list[tuple[str, str, Any]] = []
        start = _parse(start_date)
        end = _parse(end_date)
        if start:
            conditions.append((date_column, ">=", start))
        if end:
            conditions.append((date_column, "<=", end))
        return [conditions] if conditions else None

    @staticmethod
    def _load_parquet(
        path_or_url: PathLike,
        columns: Optional[Iterable[str]] = None,
        filters: Optional[list] = None,
        to_pandas_kwargs: Optional[dict] = None,
        **read_kwargs,
    ) -> pd.DataFrame:
        source = str(Path(path_or_url).expanduser())
        column_list = list(columns) if columns is not None else None

        if filters:
            table = pq.read_table(
                source, columns=column_list, filters=filters, **read_kwargs
            )
            return table.to_pandas(**(to_pandas_kwargs or {}))

        return pd.read_parquet(
            source, columns=column_list, engine="pyarrow", **read_kwargs
        )

    # ------------------------------------------------------------------ #
    # HTML-table helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_translate(value: str) -> Decimal:
        if not value:
            return Decimal("0")
        match = re.search(r"translate\(\s*[-\d.]+(?:\s*,\s*([-\d.]+))?", value)
        if not match:
            return Decimal("0")
        try:
            return Decimal(match.group(1) or "0")
        except InvalidOperation:
            return Decimal("0")

    @staticmethod
    def _read_visible_rows(table_element):
        rows: dict[Decimal, dict[str, str]] = {}

        for col in table_element.find_elements(By.CSS_SELECTOR, "g.y-column"):
            header = col.find_element(
                By.CSS_SELECTOR, "g.column-block#header text.cell-text"
            ).text.strip()

            for block in col.find_elements(
                By.CSS_SELECTOR, "g.column-block[id^='cells']"
            ):
                block_offset = HelperLibrary._parse_translate(
                    block.get_attribute("transform")
                )

                for cell in block.find_elements(By.CSS_SELECTOR, "g.column-cell"):
                    cell_offset = HelperLibrary._parse_translate(
                        cell.get_attribute("transform")
                    )
                    key = block_offset + cell_offset
                    value = cell.find_element(
                        By.CSS_SELECTOR, "text.cell-text"
                    ).text.strip()
                    rows.setdefault(key, {})[header] = value

        return rows
