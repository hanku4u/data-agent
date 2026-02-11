"""Tests for Phase 2: Architecture refactor."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from data_agent.registry import SourceRegistry
from data_agent.models import DataSourceConfig, DataSourceType, ChartType
from data_agent.charts.engine import ChartEngine
from data_agent.exceptions import SourceNotFoundError, SourceValidationError, ChartError
from data_agent.sources.csv_source import CSVSource
from data_agent.sources.sql_source import SQLSource
from data_agent.sources.base import DataSource


class TestSourceRegistry:
    def test_register_and_list(self, tmp_csv: Path):
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="test", type=DataSourceType.CSV, config={"path": str(tmp_csv)}
        )
        msg = reg.register(cfg)
        assert "registered" in msg
        assert "test" in reg.list()

    def test_get_missing_raises(self):
        reg = SourceRegistry()
        with pytest.raises(SourceNotFoundError):
            reg.get("nonexistent")

    def test_unregister(self, tmp_csv: Path):
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="test", type=DataSourceType.CSV, config={"path": str(tmp_csv)}
        )
        reg.register(cfg)
        reg.unregister("test")
        assert "test" not in reg.list()

    @pytest.mark.asyncio
    async def test_fetch_data(self, tmp_csv: Path):
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="test", type=DataSourceType.CSV, config={"path": str(tmp_csv)}
        )
        reg.register(cfg)
        result = await reg.fetch_data("test")
        assert result.row_count > 0

    @pytest.mark.asyncio
    async def test_get_schema(self, tmp_csv: Path):
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="test", type=DataSourceType.CSV, config={"path": str(tmp_csv)}
        )
        reg.register(cfg)
        schema = await reg.get_schema("test")
        assert schema.row_count > 0


class TestValidate:
    def test_base_validate_noop(self):
        """Base class validate should not raise."""

        class DummySource(DataSource):
            async def fetch(self, **kw):
                pass

            async def get_schema(self):
                pass

        ds = DummySource("test", {})
        ds.validate()  # should not raise

    def test_csv_validate_missing_file(self, tmp_path: Path):
        source = CSVSource("test", {"path": str(tmp_path / "missing.csv")})
        with pytest.raises(SourceValidationError):
            source.validate()

    def test_csv_validate_ok(self, tmp_csv: Path):
        source = CSVSource("test", {"path": str(tmp_csv)})
        source.validate()  # should not raise

    def test_sql_validate_missing_table_and_query(self, test_db: str):
        source = SQLSource("test", {"connection_string": test_db})
        with pytest.raises(SourceValidationError):
            source.validate()

    def test_registry_validates_on_register(self, tmp_path: Path):
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="bad", type=DataSourceType.CSV, config={"path": str(tmp_path / "nope.csv")}
        )
        with pytest.raises(SourceValidationError):
            reg.register(cfg)


class TestChartColumnValidation:
    def test_missing_x_column(self, numeric_df: pd.DataFrame):
        with pytest.raises(ChartError):
            ChartEngine.create_chart(
                df=numeric_df,
                chart_type=ChartType.LINE,
                x_column="nonexistent",
                y_columns=["y"],
                output_format="html",
            )

    def test_missing_y_column(self, numeric_df: pd.DataFrame):
        with pytest.raises(ChartError):
            ChartEngine.create_chart(
                df=numeric_df,
                chart_type=ChartType.LINE,
                x_column="x",
                y_columns=["nonexistent"],
                output_format="html",
            )


class TestAPIWithDependencies:
    def _make_client(self):
        from data_agent.api import app
        return TestClient(app, raise_server_exceptions=False)

    def test_health_endpoint(self, tmp_csv: Path, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("SOURCES_CONFIG", "/tmp/nonexistent.yaml")
        with TestClient(self._get_app(monkeypatch)) as client:
            resp = client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "healthy"

    def test_list_sources_empty(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("SOURCES_CONFIG", "/tmp/nonexistent.yaml")
        with TestClient(self._get_app(monkeypatch)) as client:
            resp = client.get("/data-sources")
            assert resp.status_code == 200
            assert isinstance(resp.json(), list)

    def _get_app(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("OLLAMA_BASE_URL", "http://192.168.4.210:11434")
        monkeypatch.setenv("SOURCES_CONFIG", "/tmp/nonexistent.yaml")
        # Re-import to pick up fresh app with lifespan
        import importlib
        import data_agent.api
        importlib.reload(data_agent.api)
        return data_agent.api.app
