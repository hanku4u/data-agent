"""Shared test fixtures for data-agent tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """A simple sample DataFrame."""
    return pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "value": [10, 20, 30, 40, 50],
        "category": ["A", "B", "A", "B", "A"],
    })


@pytest.fixture
def numeric_df() -> pd.DataFrame:
    """DataFrame with only numeric columns."""
    return pd.DataFrame({
        "x": [1, 2, 3, 4, 5],
        "y": [2, 4, 6, 8, 10],
        "z": [5, 4, 3, 2, 1],
    })


@pytest.fixture
def tmp_csv(sample_df: pd.DataFrame, tmp_path: Path) -> Path:
    """Write sample_df to a temporary CSV and return the path."""
    csv_path = tmp_path / "test_data.csv"
    sample_df.to_csv(csv_path, index=False)
    return csv_path


@pytest.fixture
def tmp_json(sample_df: pd.DataFrame, tmp_path: Path) -> Path:
    """Write sample_df to a temporary JSON and return the path."""
    json_path = tmp_path / "test_data.json"
    sample_df.to_json(json_path, orient="records", date_format="iso")
    return json_path


@pytest.fixture
def test_db(sample_df: pd.DataFrame, tmp_path: Path) -> str:
    """Create a temporary SQLite database and return the connection string."""
    db_path = tmp_path / "test.db"
    conn_str = f"sqlite:///{db_path}"
    engine = create_engine(conn_str)
    sample_df.to_sql("test_table", engine, index=False, if_exists="replace")
    engine.dispose()
    return conn_str


@pytest.fixture
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set environment variables for test config."""
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://192.168.4.210:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3:8b")
    monkeypatch.setenv("CHART_OUTPUT_DIR", "/tmp/test_charts")
    monkeypatch.setenv("DATA_DIR", "/tmp/test_data")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
