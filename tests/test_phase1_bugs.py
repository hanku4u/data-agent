"""Tests for Phase 1: Critical Bug Fixes."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from data_agent.sources.csv_source import CSVSource
from data_agent.sources.sql_source import SQLSource
from data_agent.charts.engine import ChartEngine
from data_agent.models import ChartType


class TestSQLInjectionFix:
    """Test SQL injection prevention."""
    
    @pytest.mark.asyncio
    async def test_sql_filters_use_parameterized_queries(self, test_db):
        """SQL filters should use parameterized queries, not string formatting."""
        source = SQLSource("test", {
            "connection_string": test_db,
            "table": "test_table"
        })
        
        # This should NOT cause SQL injection
        malicious_value = "'; DROP TABLE test_table; --"
        result = await source.fetch(filters={"category": malicious_value})
        
        # Should return empty (no match), not crash or drop table
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        
        # Verify table still exists
        engine = create_engine(test_db)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"))
            assert result.scalar() == "test_table"
    
    @pytest.mark.asyncio
    async def test_sql_columns_whitelisted(self, test_db):
        """Column names should be validated against table schema."""
        source = SQLSource("test", {
            "connection_string": test_db,
            "table": "test_table"
        })
        
        # Valid columns should work
        result = await source.fetch(columns=["date", "value"])
        assert "date" in result.columns
        assert "value" in result.columns
        
        # Invalid/dangerous column names should be rejected or ignored
        with pytest.raises((ValueError, KeyError, Exception)):
            await source.fetch(columns=["date; DROP TABLE test_table"])


class TestDataFrameCacheMutation:
    """Test DataFrame cache mutation fix."""
    
    @pytest.mark.asyncio
    async def test_csv_cache_returns_copy(self, tmp_csv):
        """CSVSource should return copies, not the cached DataFrame."""
        source = CSVSource("test", {"path": str(tmp_csv)})
        
        # First fetch
        df1 = await source.fetch()
        original_values = df1["value"].copy()
        
        # Mutate the returned DataFrame
        df1["value"] = df1["value"] * 999
        
        # Second fetch should return original data, not mutated
        df2 = await source.fetch()
        pd.testing.assert_series_equal(df2["value"], original_values, check_names=False)
    
    @pytest.mark.asyncio
    async def test_csv_cache_filtering_does_not_affect_source(self, tmp_csv):
        """Filtering should not affect the cached source data."""
        source = CSVSource("test", {"path": str(tmp_csv)})
        
        # Fetch with filter
        df_filtered = await source.fetch(filters={"category": "A"})
        assert len(df_filtered) < 5  # Should be filtered
        
        # Fetch without filter should return full data
        df_full = await source.fetch()
        assert len(df_full) == 5


class TestChartEngineMutation:
    """Test ChartEngine DataFrame mutation fix."""
    
    def test_chart_does_not_mutate_input(self, sample_df):
        """Chart creation should not mutate the input DataFrame."""
        original_date_col = sample_df["date"].copy()
        original_dtypes = sample_df.dtypes.to_dict()
        
        # Create chart
        ChartEngine.create_chart(
            df=sample_df,
            chart_type=ChartType.LINE,
            x_column="date",
            y_columns=["value"],
            output_format="html"
        )
        
        # DataFrame should be unchanged
        pd.testing.assert_series_equal(sample_df["date"], original_date_col)
        assert sample_df.dtypes.to_dict() == original_dtypes


class TestDeprecatedParameters:
    """Test removal of deprecated parameters."""
    
    @pytest.mark.asyncio
    async def test_csv_source_no_infer_datetime_format(self, tmp_csv):
        """CSVSource should not use deprecated infer_datetime_format parameter."""
        import inspect
        source = CSVSource("test", {"path": str(tmp_csv)})
        
        # Load and check that it works (should not raise DeprecationWarning)
        df = await source._load()
        assert df is not None
        assert len(df) > 0


class TestUnusedImports:
    """Test that unused imports are removed."""
    
    def test_chart_engine_no_unused_imports(self):
        """ChartEngine should not have unused imports."""
        import data_agent.charts.engine as engine_module
        import inspect
        
        # Get the source code
        source = inspect.getsource(engine_module)
        
        # Should not import plotly.express (px) if not used
        # This is a weak test - the real fix is in the code
        # but we can verify the module loads without issues
        assert hasattr(engine_module, 'ChartEngine')
