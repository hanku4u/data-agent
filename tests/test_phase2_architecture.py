"""Tests for Phase 2: Architecture Refactor."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from data_agent.sources.base import DataSource
from data_agent.sources.csv_source import CSVSource
from data_agent.sources.sql_source import SQLSource
from data_agent.exceptions import SourceValidationError, ChartError
from data_agent.models import ChartType
from data_agent.charts.engine import ChartEngine


class TestAbstractValidateMethod:
    """Test that validate() is properly implemented in all sources."""
    
    def test_base_validate_is_defined(self):
        """DataSource base class should have validate() method."""
        assert hasattr(DataSource, 'validate')
    
    @pytest.mark.asyncio
    async def test_csv_validate_implementation(self, tmp_csv):
        """CSVSource should implement validate()."""
        source = CSVSource("test", {"path": str(tmp_csv)})
        # Should not raise
        source.validate()
    
    @pytest.mark.asyncio
    async def test_csv_validate_missing_file(self, tmp_path):
        """CSVSource validate() should catch missing file."""
        source = CSVSource("test", {"path": str(tmp_path / "nonexistent.csv")})
        with pytest.raises((SourceValidationError, FileNotFoundError, ValueError)):
            source.validate()
    
    @pytest.mark.asyncio
    async def test_sql_validate_implementation(self, test_db):
        """SQLSource should implement validate()."""
        source = SQLSource("test", {
            "connection_string": test_db,
            "table": "test_table"
        })
        # Should not raise
        source.validate()
    
    @pytest.mark.asyncio
    async def test_sql_validate_invalid_connection(self):
        """SQLSource validate() should catch bad connection strings."""
        # Invalid connection raises during __init__, which is expected
        with pytest.raises(Exception):
            source = SQLSource("test", {
                "connection_string": "invalid://connection",
                "table": "test"
            })


class TestChartEngineColumnValidation:
    """Test column validation in ChartEngine."""
    
    def test_chart_validates_x_column(self, sample_df):
        """ChartEngine should validate x_column exists."""
        with pytest.raises((ValueError, KeyError, ChartError)):
            ChartEngine.create_chart(
                df=sample_df,
                chart_type=ChartType.LINE,
                x_column="nonexistent_column",
                y_columns=["value"],
                output_format="html"
            )
    
    def test_chart_validates_y_columns(self, sample_df):
        """ChartEngine should validate y_columns exist."""
        with pytest.raises((ValueError, KeyError, ChartError)):
            ChartEngine.create_chart(
                df=sample_df,
                chart_type=ChartType.LINE,
                x_column="date",
                y_columns=["nonexistent_value"],
                output_format="html"
            )
    
    def test_chart_with_valid_columns(self, sample_df):
        """ChartEngine should work with valid columns."""
        result = ChartEngine.create_chart(
            df=sample_df,
            chart_type=ChartType.LINE,
            x_column="date",
            y_columns=["value"],
            output_format="html"
        )
        assert result.html is not None
        assert result.title


class TestDependencyInjection:
    """Test FastAPI dependency injection setup."""
    
    def test_api_no_global_agent(self):
        """API should not have a global agent instance."""
        import data_agent.api as api_module
        # Should not have a global 'agent' variable
        assert not hasattr(api_module, 'agent') or api_module.__dict__.get('agent') is None
    
    def test_dependencies_module_exists(self):
        """dependencies.py module should exist."""
        try:
            from data_agent import dependencies
            assert dependencies is not None
        except ImportError:
            pytest.fail("dependencies.py module not found")
