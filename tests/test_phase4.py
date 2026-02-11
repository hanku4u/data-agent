"""Tests for Phase 4: Chart delivery & agent response pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_agent.agent import AgentDeps
from data_agent.tools.chart import ChartTool
from data_agent.tools.fetch import FetchTool
from data_agent.models import (
    AgentQueryResponse,
    ChartResult,
    DataResult,
    DataSourceConfig,
    DataSourceType,
)


class TestAgentQueryResponse:
    def test_rich_response_fields(self):
        """AgentQueryResponse should support answer, data_preview, charts, tools_used."""
        resp = AgentQueryResponse(
            answer="The data shows an upward trend.",
            sources_used=["sales"],
            data=DataResult(
                source_name="sales",
                columns=["date", "value"],
                data=[{"date": "2024-01-01", "value": 10}],
                row_count=1,
            ),
            chart=ChartResult(
                chart_type="line",
                title="Sales Trend",
                data_points=10,
                html="<div>chart</div>",
            ),
            tools_used=["fetch_data", "create_chart"],
        )
        assert resp.answer == "The data shows an upward trend."
        assert resp.chart is not None
        assert resp.chart.chart_type == "line"
        assert resp.data is not None
        assert "fetch_data" in resp.tools_used


class TestChartTool:
    def test_create_chart_html(self, sample_df):
        tool = ChartTool(output_format="html")
        result = tool.create_chart(
            data=sample_df.to_dict("records"),
            chart_type="line",
            x_column="date",
            y_columns=["value"],
            title="Test Chart",
        )
        assert result.chart_type == "line"
        assert result.html is not None
        assert result.data_points > 0

    def test_create_chart_invalid_column(self, sample_df):
        tool = ChartTool(output_format="html")
        from data_agent.exceptions import ChartError
        with pytest.raises(ChartError):
            tool.create_chart(
                data=sample_df.to_dict("records"),
                chart_type="line",
                x_column="nonexistent",
                y_columns=["value"],
            )


class TestChartResultFormats:
    def test_html_format(self, sample_df):
        tool = ChartTool(output_format="html")
        result = tool.create_chart(
            data=sample_df.to_dict("records"),
            chart_type="bar",
            x_column="date",
            y_columns=["value"],
        )
        assert result.html is not None
        assert result.image_base64 is None

    def test_json_format(self, sample_df):
        """ChartResult should be serializable to JSON."""
        tool = ChartTool(output_format="html")
        result = tool.create_chart(
            data=sample_df.to_dict("records"),
            chart_type="scatter",
            x_column="date",
            y_columns=["value"],
        )
        json_data = result.model_dump()
        assert "chart_type" in json_data
        assert "title" in json_data
        assert "data_points" in json_data
