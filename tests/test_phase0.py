"""Tests for Phase 0: exceptions, config, logging."""

from __future__ import annotations

import json
import logging

import pytest

from data_agent.exceptions import (
    DataAgentError,
    SourceNotFoundError,
    SourceValidationError,
    FetchError,
    ChartError,
    ConfigError,
)
from data_agent.config import AppConfig, LLMConfig, get_config
from data_agent.log import setup_logging, get_logger


# --- Exception tests ---

class TestExceptions:
    def test_hierarchy(self):
        """All custom exceptions inherit from DataAgentError."""
        for exc_cls in (SourceNotFoundError, SourceValidationError, FetchError, ChartError, ConfigError):
            assert issubclass(exc_cls, DataAgentError)

    def test_source_not_found_message(self):
        err = SourceNotFoundError("sales", available=["metrics", "users"])
        assert "sales" in str(err)
        assert err.source_name == "sales"
        assert "metrics" in err.message

    def test_data_agent_error_fields(self):
        err = DataAgentError(message="boom", detail="ctx")
        assert err.message == "boom"
        assert err.detail == "ctx"

    def test_catch_base(self):
        with pytest.raises(DataAgentError):
            raise FetchError("connection failed")


# --- Config tests ---

class TestConfig:
    def test_defaults(self):
        cfg = LLMConfig()
        assert cfg.provider in ("openai", "ollama", "anthropic")
        assert isinstance(cfg.ollama_model, str)

    def test_env_override(self, mock_env):
        cfg = LLMConfig()
        assert cfg.provider == "ollama"
        assert cfg.ollama_base_url == "http://192.168.4.210:11434"

    def test_app_config_env(self, mock_env):
        cfg = AppConfig()
        assert cfg.chart_output_dir == "/tmp/test_charts"
        assert cfg.log_level == "DEBUG"

    def test_get_config(self, mock_env):
        cfg = get_config()
        assert isinstance(cfg, AppConfig)
        assert cfg.llm.provider == "ollama"


# --- Logging tests ---

class TestLogging:
    def test_setup_logging_no_crash(self):
        setup_logging(level="DEBUG", json_output=True)
        setup_logging(level="INFO", json_output=False)

    def test_get_logger(self):
        setup_logging()
        log = get_logger("test")
        assert log is not None

    def test_json_output(self, capsys):
        setup_logging(level="DEBUG", json_output=True)
        log = get_logger("test_json")
        log.info("hello", foo="bar")
        captured = capsys.readouterr()
        # Should be parseable JSON
        parsed = json.loads(captured.out.strip())
        assert parsed["event"] == "hello"
        assert parsed["foo"] == "bar"
        assert "timestamp" in parsed
        assert "level" in parsed
