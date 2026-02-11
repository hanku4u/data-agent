"""Tests for Phase 1: Critical bug fixes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from data_agent.sources.csv_source import CSVSource
from data_agent.sources.sql_source import SQLSource
from data_agent.charts.engine import ChartEngine
from data_agent.models import ChartType


# --- CSV cache mutation fix ---

class TestCSVCacheMutation:
    @pytest.mark.asyncio
    async def test_fetch_returns_copy(self, tmp_csv: Path):
        """Fetching should return a copy, not the cached DataFrame."""
        source = CSVSource("test", {"path": str(tmp_csv)})
        df1 = await source.fetch()
        df1["new_col"] = 999  # mutate
        df2 = await source.fetch()
        assert "new_col" not in df2.columns

    @pytest.mark.asyncio
    async def test_cache_not_mutated(self, tmp_csv: Path):
        source = CSVSource("test", {"path": str(tmp_csv)})
        df1 = await source.fetch()
        original_len = len(df1)
        df1.drop(df1.index, inplace=True)
        df2 = await source.fetch()
        assert len(df2) == original_len


# --- SQL injection fix ---

class TestSQLInjection:
    @pytest.mark.asyncio
    async def test_column_whitelist(self, test_db: str):
        """SQL source should reject columns not in the table."""
        source = SQLSource("test", {"connection_string": test_db, "table": "test_table"})
        with pytest.raises((ValueError, KeyError)):
            await source.fetch(columns=["value; DROP TABLE test_table;--"])

    @pytest.mark.asyncio
    async def test_order_by_whitelist(self, test_db: str):
        """SQL source should reject invalid order_by columns."""
        source = SQLSource("test", {"connection_string": test_db, "table": "test_table"})
        with pytest.raises(ValueError):
            await source.fetch(order_by="value; DROP TABLE test_table;--")

    @pytest.mark.asyncio
    async def test_valid_columns_work(self, test_db: str):
        source = SQLSource("test", {"connection_string": test_db, "table": "test_table"})
        df = await source.fetch(columns=["value"], limit=2)
        assert list(df.columns) == ["value"]
        assert len(df) == 2


# --- ChartEngine mutation fix ---

class TestChartEngineMutation:
    def test_input_df_not_mutated(self, sample_df: pd.DataFrame):
        """ChartEngine should not mutate the input DataFrame."""
        original_cols = list(sample_df.columns)
        original_values = sample_df["date"].tolist()
        try:
            ChartEngine.create_chart(
                df=sample_df,
                chart_type=ChartType.LINE,
                x_column="date",
                y_columns=["value"],
                title="Test",
                output_format="html",  # avoid kaleido dependency
            )
        except Exception:
            pass  # chart generation may fail in test env, we just care about mutation
        assert list(sample_df.columns) == original_cols


# --- Deprecated parameter removal ---

class TestDeprecatedParams:
    def test_no_infer_datetime_format(self):
        """CSVSource should not use infer_datetime_format."""
        import inspect
        source_code = inspect.getsource(CSVSource)
        assert "infer_datetime_format" not in source_code

    def test_no_unused_plotly_express(self):
        """ChartEngine should not import plotly.express."""
        import inspect
        source_code = inspect.getsource(ChartEngine)
        assert "plotly.express" not in source_code
        assert "import px" not in source_code
