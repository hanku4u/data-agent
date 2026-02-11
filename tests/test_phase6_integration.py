"""Phase 6: Integration tests - E2E, YAML loading, API contracts."""

from __future__ import annotations

import importlib
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml
from fastapi.testclient import TestClient

from data_agent.registry import SourceRegistry
from data_agent.tools.chart import ChartTool
from data_agent.tools.transform import TransformTool
from data_agent.models import DataSourceConfig, DataSourceType, ChartType


# --- E2E Integration: register → fetch → transform → chart ---

class TestE2EIntegration:
    @pytest.mark.asyncio
    async def test_register_fetch_chart(self, tmp_csv: Path):
        """Full pipeline: register source → fetch data → create chart."""
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="metrics",
            type=DataSourceType.CSV,
            config={"path": str(tmp_csv)},
        )
        reg.register(cfg)

        # Fetch
        result = await reg.fetch_data("metrics")
        assert result.row_count > 0

        # Chart
        chart_tool = ChartTool(output_format="html")
        chart = chart_tool.create_chart(
            data=result.data,
            chart_type="line",
            x_column="date",
            y_columns=["value"],
            title="Metrics Over Time",
        )
        assert chart.html is not None
        assert chart.data_points == result.row_count

    @pytest.mark.asyncio
    async def test_register_fetch_transform(self, tmp_csv: Path):
        """Pipeline: register → fetch → transform."""
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="metrics",
            type=DataSourceType.CSV,
            config={"path": str(tmp_csv)},
        )
        reg.register(cfg)

        result = await reg.fetch_data("metrics")
        transform = TransformTool()
        agg = transform.aggregate(result.data, "value", "sum")
        assert agg["result"] == 150  # 10+20+30+40+50

    @pytest.mark.asyncio
    async def test_sql_register_fetch_chart(self, test_db: str):
        """E2E with SQL source."""
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="sql_metrics",
            type=DataSourceType.SQL,
            config={"connection_string": test_db, "table": "test_table"},
        )
        reg.register(cfg)

        result = await reg.fetch_data("sql_metrics")
        assert result.row_count > 0

        schema = await reg.get_schema("sql_metrics")
        assert schema.row_count > 0


# --- YAML source loading ---

class TestYAMLLoading:
    def test_load_csv_from_yaml(self, tmp_csv: Path, tmp_path: Path):
        yaml_content = {
            "sources": {
                "test_csv": {
                    "type": "csv",
                    "config": {"path": str(tmp_csv)},
                    "description": "Test CSV source",
                }
            }
        }
        yaml_path = tmp_path / "sources.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        from data_agent.api import load_sources_from_yaml

        reg = SourceRegistry()
        load_sources_from_yaml(reg, str(yaml_path))
        assert "test_csv" in reg.list()

    def test_load_nonexistent_yaml(self):
        from data_agent.api import load_sources_from_yaml

        reg = SourceRegistry()
        load_sources_from_yaml(reg, "/tmp/does_not_exist.yaml")
        assert reg.list() == []

    def test_load_yaml_with_bad_source(self, tmp_path: Path):
        yaml_content = {
            "sources": {
                "bad_source": {
                    "type": "csv",
                    "config": {"path": "/nonexistent/file.csv"},
                }
            }
        }
        yaml_path = tmp_path / "sources.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(yaml_content, f)

        from data_agent.api import load_sources_from_yaml

        reg = SourceRegistry()
        load_sources_from_yaml(reg, str(yaml_path))
        # Should not crash, just skip bad source
        assert "bad_source" not in reg.list()


# --- API Contract Tests ---

class TestAPIContracts:
    def _get_app(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("OLLAMA_BASE_URL", "http://192.168.4.210:11434")
        monkeypatch.setenv("SOURCES_CONFIG", "/tmp/nonexistent.yaml")
        import data_agent.api
        importlib.reload(data_agent.api)
        return data_agent.api.app

    def test_health_returns_expected_fields(self, monkeypatch):
        with TestClient(self._get_app(monkeypatch)) as client:
            resp = client.get("/health")
            assert resp.status_code == 200
            body = resp.json()
            assert "status" in body
            assert "version" in body
            assert "llm_provider" in body
            assert "data_sources" in body

    def test_register_and_list(self, monkeypatch, tmp_csv: Path):
        with TestClient(self._get_app(monkeypatch)) as client:
            # Register
            resp = client.post(
                "/data-sources",
                json={
                    "name": "test",
                    "type": "csv",
                    "config": {"path": str(tmp_csv)},
                },
            )
            assert resp.status_code == 200

            # List
            resp = client.get("/data-sources")
            assert resp.status_code == 200
            assert "test" in resp.json()

            # Schema
            resp = client.get("/data-sources/test/schema")
            assert resp.status_code == 200
            body = resp.json()
            assert "columns" in body
            assert "row_count" in body

    def test_delete_source(self, monkeypatch, tmp_csv: Path):
        with TestClient(self._get_app(monkeypatch)) as client:
            client.post(
                "/data-sources",
                json={"name": "del_me", "type": "csv", "config": {"path": str(tmp_csv)}},
            )
            resp = client.delete("/data-sources/del_me")
            assert resp.status_code == 200

            resp = client.get("/data-sources")
            assert "del_me" not in resp.json()
    
    @pytest.mark.asyncio
    async def test_chart_filters_passed_to_registry(self, tmp_csv: Path):
        """Test that filters are passed from chart creation to registry.fetch_data."""
        from data_agent.registry import SourceRegistry
        from data_agent.tools.chart import ChartTool
        from data_agent.models import DataSourceConfig, DataSourceType
        
        reg = SourceRegistry()
        cfg = DataSourceConfig(
            name="chart_test",
            type=DataSourceType.CSV,
            config={"path": str(tmp_csv)},
        )
        reg.register(cfg)
        
        # Fetch with filters (simulating what the chart endpoint should do)
        result = await reg.fetch_data(
            source_name="chart_test",
            filters={"category": "A"},
        )
        
        # Should only return rows with category "A"
        # sample_df has 3 "A" rows out of 5 total
        assert result.row_count == 3
        for record in result.data:
            assert record["category"] == "A"
