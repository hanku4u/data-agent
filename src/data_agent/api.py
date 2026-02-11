"""FastAPI application exposing the Data Agent."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import List, Optional

import yaml
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .agent import create_agent, AgentDeps
from .config import get_config
from .tools.fetch import FetchTool
from .tools.chart import ChartTool
from .models import (
    AgentQueryRequest,
    AgentQueryResponse,
    DataSourceConfig,
    DataSourceInfo,
    ChartRequest,
    ChartResult,
)

# Global state
fetch_tool = FetchTool()
chart_tool = ChartTool()
config = get_config()


def load_sources_from_yaml(path: str) -> None:
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
                description=source_def.get("description", "")
            )
            fetch_tool.register_source(source_config)
            print(f"  ✅ Loaded source: {name} ({source_def['type']})")
        except Exception as e:
            print(f"  ⚠️  Failed to load source '{name}': {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: load sources on startup."""
    print("🚀 Starting Data Agent...")
    print(f"📡 LLM Provider: {config.llm.provider}")
    
    # Load sources from YAML config
    print(f"📂 Loading sources from {config.sources_config_path}...")
    load_sources_from_yaml(config.sources_config_path)
    
    loaded = fetch_tool.list_sources()
    print(f"📊 {len(loaded)} data source(s) loaded: {', '.join(loaded) or 'none'}")
    print("✅ Data Agent ready!")
    
    yield
    
    print("👋 Shutting down Data Agent...")


# Create FastAPI app
app = FastAPI(
    title="Data Agent API",
    description="AI-powered data fetching and chart generation agent",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "0.1.0",
        "llm_provider": config.llm.provider,
        "data_sources": fetch_tool.list_sources()
    }


# --- Agent Endpoints ---

@app.post("/agent/query", response_model=AgentQueryResponse)
async def agent_query(request: AgentQueryRequest):
    """
    Query the AI agent with natural language.
    
    The agent will use its tools to fetch data and create charts
    based on your query.
    """
    try:
        agent = create_agent(config.llm)
        deps = AgentDeps(fetch_tool=fetch_tool, chart_tool=chart_tool)
        
        result = await agent.run(request.query, deps=deps)
        
        return AgentQueryResponse(
            answer=result.data,
            sources_used=fetch_tool.list_sources()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Data Source Endpoints ---

@app.get("/data-sources", response_model=List[str])
async def list_data_sources():
    """List all registered data sources."""
    return fetch_tool.list_sources()


@app.get("/data-sources/{name}/schema")
async def get_source_schema(name: str):
    """Get schema information for a data source."""
    try:
        schema = await fetch_tool.get_schema(name)
        return schema.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/data-sources", response_model=str)
async def register_data_source(config: DataSourceConfig):
    """Register a new data source."""
    try:
        return fetch_tool.register_source(config)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/data-sources/{name}")
async def remove_data_source(name: str):
    """Remove a registered data source."""
    return {"message": fetch_tool.unregister_source(name)}


# --- Direct Chart Endpoint ---

@app.post("/agent/chart")
async def create_chart(request: ChartRequest):
    """
    Generate a chart directly from a data source.
    
    Bypasses the AI agent for direct chart generation.
    """
    try:
        result = await fetch_tool.fetch_data(
            source_name=request.data_source,
            limit=request.limit,
            order_by=request.x_column
        )
        
        if not result.data:
            raise HTTPException(status_code=404, detail="No data found")
        
        chart_result = chart_tool.create_chart(
            data=result.data,
            chart_type=request.chart_type.value,
            x_column=request.x_column,
            y_columns=request.y_columns,
            title=request.title,
            x_label=request.x_label,
            y_label=request.y_label
        )
        
        return chart_result.model_dump()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
