"""FastAPI dependency injection providers."""

from __future__ import annotations

from fastapi import Request

from .registry import SourceRegistry
from .tools.chart import ChartTool
from .config import AppConfig


def get_registry(request: Request) -> SourceRegistry:
    """Get the source registry from app state."""
    return request.app.state.registry


def get_chart_tool(request: Request) -> ChartTool:
    """Get the chart tool from app state."""
    return request.app.state.chart_tool


def get_app_config(request: Request) -> AppConfig:
    """Get app config from app state."""
    return request.app.state.config
