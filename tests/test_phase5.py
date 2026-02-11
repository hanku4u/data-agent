"""Tests for Phase 5: Data transformation tool."""

from __future__ import annotations

import pandas as pd
import pytest

from data_agent.tools.transform import TransformTool


@pytest.fixture
def time_series_df():
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=20, freq="D"),
        "value": list(range(20)),
        "category": ["A", "B"] * 10,
    })


class TestGroupBy:
    def test_groupby_sum(self, time_series_df):
        tool = TransformTool()
        result = tool.groupby(
            data=time_series_df.to_dict("records"),
            group_columns=["category"],
            agg_column="value",
            agg_func="sum",
        )
        assert len(result) == 2
        # A = 0+2+4+...+18 = 90, B = 1+3+5+...+19 = 100
        for row in result:
            if row["category"] == "A":
                assert row["value"] == 90
            elif row["category"] == "B":
                assert row["value"] == 100

    def test_groupby_mean(self, time_series_df):
        tool = TransformTool()
        result = tool.groupby(
            data=time_series_df.to_dict("records"),
            group_columns=["category"],
            agg_column="value",
            agg_func="mean",
        )
        assert len(result) == 2


class TestRollingAverage:
    def test_rolling_average(self, time_series_df):
        tool = TransformTool()
        result = tool.rolling_average(
            data=time_series_df.to_dict("records"),
            column="value",
            window=3,
        )
        assert len(result) == 20
        # First two should be NaN (dropped or NaN)
        assert result[0].get("value_rolling_3") is None or pd.isna(result[0].get("value_rolling_3"))

    def test_rolling_adds_column(self, time_series_df):
        tool = TransformTool()
        result = tool.rolling_average(
            data=time_series_df.to_dict("records"),
            column="value",
            window=5,
        )
        assert "value_rolling_5" in result[0]


class TestAggregate:
    def test_aggregate_sum(self, time_series_df):
        tool = TransformTool()
        result = tool.aggregate(
            data=time_series_df.to_dict("records"),
            column="value",
            func="sum",
        )
        assert result == {"column": "value", "func": "sum", "result": 190}

    def test_aggregate_mean(self, time_series_df):
        tool = TransformTool()
        result = tool.aggregate(
            data=time_series_df.to_dict("records"),
            column="value",
            func="mean",
        )
        assert result["result"] == 9.5


class TestResample:
    def test_resample_weekly(self, time_series_df):
        tool = TransformTool()
        result = tool.resample(
            data=time_series_df.to_dict("records"),
            date_column="date",
            freq="W",
            agg_column="value",
            agg_func="sum",
        )
        assert len(result) > 0
        assert len(result) < 20  # should be fewer rows after resampling
