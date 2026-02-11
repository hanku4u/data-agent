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
    
    def test_rolling_nan_only_affects_rolling_column(self):
        """Test that NaN conversion only affects the rolling column, not others."""
        tool = TransformTool()
        
        # Create data with some existing NaN values in other columns
        data = [
            {"date": "2024-01-01", "value": 10, "other": 1.0},
            {"date": "2024-01-02", "value": 20, "other": None},
            {"date": "2024-01-03", "value": 30, "other": 3.0},
            {"date": "2024-01-04", "value": 40, "other": None},
            {"date": "2024-01-05", "value": 50, "other": 5.0},
        ]
        
        result = tool.rolling_average(data=data, column="value", window=3)
        
        # Check that original NaN/None values in 'other' remain as NaN (pandas behavior)
        assert pd.isna(result[1]["other"])
        assert pd.isna(result[3]["other"])
        
        # Check that non-None values in 'other' are preserved
        assert result[0]["other"] == 1.0
        assert result[2]["other"] == 3.0
        
        # Check that rolling column has None (not NaN) for incomplete windows
        assert result[0]["value_rolling_3"] is None  # First window incomplete
        assert result[1]["value_rolling_3"] is None  # Second window incomplete


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


class TestAggFuncValidation:
    """Test that aggregation functions are validated against whitelist."""
    
    def test_groupby_invalid_agg_func(self, time_series_df):
        tool = TransformTool()
        with pytest.raises(ValueError, match="Invalid aggregation function"):
            tool.groupby(
                data=time_series_df.to_dict("records"),
                group_columns=["category"],
                agg_column="value",
                agg_func="dangerous_func",
            )
    
    def test_resample_invalid_agg_func(self, time_series_df):
        tool = TransformTool()
        with pytest.raises(ValueError, match="Invalid aggregation function"):
            tool.resample(
                data=time_series_df.to_dict("records"),
                date_column="date",
                freq="W",
                agg_column="value",
                agg_func="eval",
            )
    
    def test_aggregate_invalid_func(self, time_series_df):
        tool = TransformTool()
        with pytest.raises(ValueError, match="Invalid aggregation function"):
            tool.aggregate(
                data=time_series_df.to_dict("records"),
                column="value",
                func="__import__",
            )
    
    def test_all_allowed_funcs_work(self, time_series_df):
        tool = TransformTool()
        data = time_series_df.to_dict("records")
        
        # Test that all whitelisted functions work
        for func in TransformTool.ALLOWED_AGG_FUNCS:
            result = tool.aggregate(data=data, column="value", func=func)
            assert result["func"] == func
            assert "result" in result


class TestAggregateReturnType:
    """Test that aggregate returns JSON-serializable types."""
    
    def test_aggregate_returns_python_float(self, time_series_df):
        tool = TransformTool()
        result = tool.aggregate(
            data=time_series_df.to_dict("records"),
            column="value",
            func="mean",
        )
        # Should be Python float, not numpy scalar
        assert isinstance(result["result"], float)
    
    def test_aggregate_count_returns_int(self, time_series_df):
        tool = TransformTool()
        result = tool.aggregate(
            data=time_series_df.to_dict("records"),
            column="value",
            func="count",
        )
        # Count should return int
        assert isinstance(result["result"], int)
    
    def test_aggregate_result_is_json_serializable(self, time_series_df):
        import json
        tool = TransformTool()
        
        for func in ["sum", "mean", "count", "min", "max", "std"]:
            result = tool.aggregate(
                data=time_series_df.to_dict("records"),
                column="value",
                func=func,
            )
            # Should not raise TypeError
            json_str = json.dumps(result)
            assert json_str is not None
