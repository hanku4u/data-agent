"""FastAPI application exposing the Data Agent."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import List, Optional

import yaml
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends

from .agent import create_agent, AgentDeps
from .config import AppConfig, get_config
from .registry import SourceRegistry
from .dependencies import get_registry, get_chart_tool, get_app_config
from .tools.chart import ChartTool
from .log import setup_logging, get_logger
from .middleware import RequestLoggingMiddleware, data_agent_exception_handler
from .exceptions import DataAgentError, SourceNotFoundError, FetchError
from .models import (
    AgentQueryRequest,
    AgentQueryResponse,
    DataSourceConfig,
    DataSourceInfo,
    ChartRequest,
    ChartResult,
)

logger = get_logger(__name__)


def load_sources_from_yaml(registry: SourceRegistry, path: str) -> None:
    """Load data sources from a YAML configuration file."""
    config_path = Path(path)
    if not config_path.exists():
        return

    with open(config_path) as f:
        sources_config = yaml.safe_load(f)

    if not sources_config or "sources" not in sources_config:
        return

    for name, source_def in sources_config["sources"].items():
        try:
            source_config = DataSourceConfig(
                name=name,
                type=source_def["type"],
                config=source_def.get("config", {}),
                description=source_def.get("description", ""),
            )
            registry.register(source_config)
            logger.info("source_loaded", source_name=name, source_type=source_def["type"])
        except Exception as e:
            logger.warning("source_load_failed", source_name=name, error=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize state on startup."""
    config = get_config()
    setup_logging(level=config.log_level, json_output=config.log_json)

    registry = SourceRegistry()
    chart_tool = ChartTool()

    # Store on app.state
    app.state.config = config
    app.state.registry = registry
    app.state.chart_tool = chart_tool

    # Create agent once at startup
    app.state.agent = create_agent(config.llm)

    logger.info("startup", llm_provider=config.llm.provider)

    # Load sources from YAML
    load_sources_from_yaml(registry, config.sources_config_path)

    loaded = registry.list()
    logger.info("sources_loaded", count=len(loaded), names=loaded)

    yield

    logger.info("shutdown")


app = FastAPI(
    title="Data Agent API",
    description="AI-powered data fetching and chart generation agent",
    version="0.1.0",
    lifespan=lifespan,
)

# Register middleware and exception handlers
app.add_middleware(RequestLoggingMiddleware)
app.add_exception_handler(DataAgentError, data_agent_exception_handler)


@app.get("/health")
async def health_check(
    registry: SourceRegistry = Depends(get_registry),
    config: AppConfig = Depends(get_app_config),
):
    return {
        "status": "healthy",
        "version": "0.1.0",
        "llm_provider": config.llm.provider,
        "data_sources": registry.list(),
    }


@app.post("/agent/query", response_model=AgentQueryResponse)
async def agent_query(
    request: AgentQueryRequest,
    registry: SourceRegistry = Depends(get_registry),
    chart_tool: ChartTool = Depends(get_chart_tool),
    config: AppConfig = Depends(get_app_config),
):
    try:
        agent = app.state.agent
        deps = AgentDeps(registry=registry, chart_tool=chart_tool)

        result = await agent.run(request.query, deps=deps)

        return AgentQueryResponse(
            answer=result.data,
            sources_used=registry.list(),
        )
    except Exception as e:
        logger.error("agent_query_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/data-sources", response_model=List[str])
async def list_data_sources(registry: SourceRegistry = Depends(get_registry)):
    return registry.list()


@app.get("/data-sources/{name}/schema")
async def get_source_schema(
    name: str, registry: SourceRegistry = Depends(get_registry)
):
    schema = await registry.get_schema(name)
    return schema.model_dump()


@app.post("/data-sources", response_model=str)
async def register_data_source(
    config: DataSourceConfig, registry: SourceRegistry = Depends(get_registry)
):
    return registry.register(config)


@app.delete("/data-sources/{name}")
async def remove_data_source(
    name: str, registry: SourceRegistry = Depends(get_registry)
):
    return {"message": registry.unregister(name)}


@app.post("/agent/chart")
async def create_chart(
    request: ChartRequest,
    registry: SourceRegistry = Depends(get_registry),
    chart_tool: ChartTool = Depends(get_chart_tool),
):
    try:
        result = await registry.fetch_data(
            source_name=request.data_source,
            filters=request.filters,
            limit=request.limit,
            order_by=request.x_column,
        )

        if not result.data:
            raise HTTPException(status_code=404, detail="No data found")

        chart_result = await asyncio.to_thread(
            chart_tool.create_chart,
            data=result.data,
            chart_type=request.chart_type.value,
            x_column=request.x_column,
            y_columns=request.y_columns,
            title=request.title,
            x_label=request.x_label,
            y_label=request.y_label,
        )

        return chart_result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
