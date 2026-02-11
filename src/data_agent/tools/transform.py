"""Data transformation tool — groupby, resample, rolling average, aggregate."""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..exceptions import FetchError


class TransformTool:
    """Provides data transformation operations for the agent."""

    def groupby(
        self,
        data: list[dict[str, Any]],
        group_columns: list[str],
        agg_column: str,
        agg_func: str = "sum",
    ) -> list[dict[str, Any]]:
        """Group data by columns and aggregate.

        Args:
            data: List of records.
            group_columns: Columns to group by.
            agg_column: Column to aggregate.
            agg_func: Aggregation function (sum, mean, count, min, max).

        Returns:
            Grouped and aggregated records.
        """
        df = pd.DataFrame(data)
        result = df.groupby(group_columns, as_index=False).agg({agg_column: agg_func})
        return result.to_dict("records")

    def resample(
        self,
        data: list[dict[str, Any]],
        date_column: str,
        freq: str,
        agg_column: str,
        agg_func: str = "sum",
    ) -> list[dict[str, Any]]:
        """Resample time-series data to a different frequency.

        Args:
            data: List of records.
            date_column: Column containing dates.
            freq: Pandas frequency string (D, W, M, Q, Y).
            agg_column: Column to aggregate.
            agg_func: Aggregation function.

        Returns:
            Resampled records.
        """
        df = pd.DataFrame(data)
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.set_index(date_column)
        result = df.resample(freq).agg({agg_column: agg_func}).reset_index()
        return result.to_dict("records")

    def rolling_average(
        self,
        data: list[dict[str, Any]],
        column: str,
        window: int = 7,
    ) -> list[dict[str, Any]]:
        """Calculate a rolling average.

        Args:
            data: List of records.
            column: Column to calculate rolling average on.
            window: Window size.

        Returns:
            Records with added rolling average column.
        """
        df = pd.DataFrame(data)
        col_name = f"{column}_rolling_{window}"
        df[col_name] = df[column].rolling(window=window).mean()
        # Convert NaN to None for JSON serialization
        return df.where(df.notna(), None).to_dict("records")

    def aggregate(
        self,
        data: list[dict[str, Any]],
        column: str,
        func: str = "sum",
    ) -> dict[str, Any]:
        """Compute a single aggregate value.

        Args:
            data: List of records.
            column: Column to aggregate.
            func: Aggregation function (sum, mean, count, min, max, std).

        Returns:
            Dict with column, func, and result.
        """
        df = pd.DataFrame(data)
        result = getattr(df[column], func)()
        return {"column": column, "func": func, "result": result}
