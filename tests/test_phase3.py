"""Tests for Phase 3: Error handling & middleware."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import PlainTextResponse

from data_agent.middleware import (
    RequestLoggingMiddleware,
    data_agent_exception_handler,
    _EXCEPTION_STATUS_MAP,
)
from data_agent.exceptions import (
    DataAgentError,
    SourceNotFoundError,
    SourceValidationError,
    FetchError,
    ChartError,
    ConfigError,
)


class TestExceptionStatusMapping:
    def test_source_not_found_is_404(self):
        assert _EXCEPTION_STATUS_MAP[SourceNotFoundError] == 404

    def test_validation_is_400(self):
        assert _EXCEPTION_STATUS_MAP[SourceValidationError] == 400

    def test_fetch_is_500(self):
        assert _EXCEPTION_STATUS_MAP[FetchError] == 500

    def test_chart_is_400(self):
        assert _EXCEPTION_STATUS_MAP[ChartError] == 400

    def test_config_is_500(self):
        assert _EXCEPTION_STATUS_MAP[ConfigError] == 500


class TestMiddleware:
    def _make_app(self):
        app = FastAPI()
        app.add_middleware(RequestLoggingMiddleware)
        app.add_exception_handler(DataAgentError, data_agent_exception_handler)

        @app.get("/ok")
        async def ok():
            return {"status": "ok"}

        @app.get("/not-found")
        async def not_found():
            raise SourceNotFoundError("test_source")

        @app.get("/validation-error")
        async def validation_error():
            raise SourceValidationError("bad config")

        return app

    def test_request_id_header(self):
        client = TestClient(self._make_app())
        resp = client.get("/ok")
        assert resp.status_code == 200
        assert "X-Request-ID" in resp.headers

    def test_404_on_source_not_found(self):
        client = TestClient(self._make_app(), raise_server_exceptions=False)
        resp = client.get("/not-found")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "SourceNotFoundError"
        assert "request_id" in body

    def test_400_on_validation_error(self):
        client = TestClient(self._make_app(), raise_server_exceptions=False)
        resp = client.get("/validation-error")
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "SourceValidationError"


class TestAPIErrorHandling:
    def _get_app(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "ollama")
        monkeypatch.setenv("OLLAMA_BASE_URL", "http://192.168.4.210:11434")
        monkeypatch.setenv("SOURCES_CONFIG", "/tmp/nonexistent.yaml")
        import importlib
        import data_agent.api
        importlib.reload(data_agent.api)
        return data_agent.api.app

    def test_schema_missing_source_returns_404(self, monkeypatch):
        app = self._get_app(monkeypatch)
        with TestClient(app, raise_server_exceptions=False) as client:
            resp = client.get("/data-sources/nonexistent/schema")
            assert resp.status_code == 404
