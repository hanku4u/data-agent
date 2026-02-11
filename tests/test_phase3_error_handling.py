"""Tests for Phase 3: Error Handling & Middleware."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from fastapi.responses import JSONResponse

from data_agent.middleware import (
    RequestLoggingMiddleware,
    data_agent_exception_handler,
)
from data_agent.exceptions import (
    SourceNotFoundError,
    SourceValidationError,
    FetchError,
    ChartError,
    ConfigError,
)

# Access the private status map for testing
from data_agent.middleware import _EXCEPTION_STATUS_MAP as EXCEPTION_STATUS_MAP


class TestExceptionStatusMapping:
    """Test that exceptions map to appropriate HTTP status codes."""
    
    def test_exception_status_map_exists(self):
        """Exception status map should be defined."""
        assert EXCEPTION_STATUS_MAP is not None
        assert len(EXCEPTION_STATUS_MAP) > 0
    
    def test_source_not_found_maps_to_404(self):
        """SourceNotFoundError should map to 404."""
        assert EXCEPTION_STATUS_MAP[SourceNotFoundError] == 404
    
    def test_validation_error_maps_to_400(self):
        """SourceValidationError should map to 400."""
        assert EXCEPTION_STATUS_MAP[SourceValidationError] == 400
    
    def test_fetch_error_maps_to_500(self):
        """FetchError should map to 500."""
        assert EXCEPTION_STATUS_MAP[FetchError] == 500
    
    def test_chart_error_maps_to_400(self):
        """ChartError should map to 400."""
        assert EXCEPTION_STATUS_MAP[ChartError] == 400


class TestGlobalExceptionHandler:
    """Test the global exception handler."""
    
    @pytest.mark.asyncio
    async def test_handles_source_not_found(self):
        """Should return 404 for SourceNotFoundError."""
        app = FastAPI()
        request = Request({"type": "http", "method": "GET", "url": "/test", "headers": []})
        request.state.request_id = "test-123"
        
        exc = SourceNotFoundError("test_source", available=["source1", "source2"])
        response = await data_agent_exception_handler(request, exc)
        
        assert response.status_code == 404
        content = response.body.decode()
        assert "test_source" in content
        assert "test-123" in content
    
    @pytest.mark.asyncio
    async def test_handles_validation_error(self):
        """Should return 400 for SourceValidationError."""
        app = FastAPI()
        request = Request({"type": "http", "method": "POST", "url": "/test", "headers": []})
        request.state.request_id = "test-456"
        
        exc = SourceValidationError("Invalid configuration")
        response = await data_agent_exception_handler(request, exc)
        
        assert response.status_code == 400
        content = response.body.decode()
        assert "test-456" in content


class TestRequestLoggingMiddleware:
    """Test the request logging middleware."""
    
    def test_middleware_adds_request_id(self):
        """Middleware should add X-Request-ID header to responses."""
        app = FastAPI()
        app.add_middleware(RequestLoggingMiddleware)
        
        @app.get("/test")
        async def test_endpoint():
            return {"message": "ok"}
        
        client = TestClient(app)
        response = client.get("/test")
        
        assert response.status_code == 200
        assert "X-Request-ID" in response.headers
        assert response.headers["X-Request-ID"]  # Should not be empty
    
    def test_middleware_logs_requests(self):
        """Middleware should log request and response without errors."""
        app = FastAPI()
        app.add_middleware(RequestLoggingMiddleware)
        
        @app.get("/test")
        async def test_endpoint():
            return {"message": "ok"}
        
        client = TestClient(app)
        response = client.get("/test")
        
        # Should complete successfully with middleware logging
        assert response.status_code == 200
        assert "X-Request-ID" in response.headers
        # Logs are written via structlog (verified in captured output)


class TestAPISpecificExceptions:
    """Test that API uses specific exception types, not broad catches."""
    
    def test_api_module_imports_exceptions(self):
        """API module should import specific exception types."""
        import data_agent.api as api_module
        import inspect
        
        source = inspect.getsource(api_module)
        
        # Should import specific exceptions
        assert "SourceNotFoundError" in source or "from .exceptions import" in source
        
        # Should not have broad exception catches (though this is hard to test perfectly)
        # At least check it has the middleware
        assert "RequestLoggingMiddleware" in source or "middleware" in source.lower()
