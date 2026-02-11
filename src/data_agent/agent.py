"""Pydantic AI agent definition with data fetch and chart tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent, RunContext

from .config import LLMConfig, get_config
from .tools.fetch import FetchTool
from .tools.chart import ChartTool
from .models import DataResult, DataSchema, ChartResult, ChartType


@dataclass
class AgentDeps:
    """Dependencies injected into the agent at runtime."""
    fetch_tool: FetchTool
    chart_tool: ChartTool


def get_model_string(llm_config: LLMConfig) -> str:
    """Convert LLM config to a Pydantic AI model string."""
    provider = llm_config.provider.lower()
    
    if provider == "openai":
        return f"openai:{llm_config.openai_model}"
    elif provider == "anthropic":
        return f"anthropic:{llm_config.anthropic_model}"
    elif provider == "ollama":
        return f"ollama:{llm_config.ollama_model}"
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")


def create_agent(llm_config: Optional[LLMConfig] = None) -> Agent[AgentDeps, str]:
    """
    Create a Pydantic AI agent with data tools.
    
    Args:
        llm_config: LLM configuration (uses env vars if not provided)
    
    Returns:
        Configured Pydantic AI agent
    """
    if llm_config is None:
        llm_config = get_config().llm
    
    model = get_model_string(llm_config)
    
    agent = Agent(
        model,
        deps_type=AgentDeps,
        system_prompt=SYSTEM_PROMPT,
        retries=2
    )
    
    # --- Register tools ---
    
    @agent.tool
    async def list_data_sources(ctx: RunContext[AgentDeps]) -> str:
        """List all available data sources."""
        sources = ctx.deps.fetch_tool.list_sources()
        if not sources:
            return "No data sources registered. Ask the user to register one."
        return f"Available data sources: {', '.join(sources)}"
    
    @agent.tool
    async def get_data_schema(ctx: RunContext[AgentDeps], source_name: str) -> str:
        """
        Get schema information for a data source (column names, types, sample values).
        
        Args:
            source_name: Name of the data source
        """
        try:
            schema = await ctx.deps.fetch_tool.get_schema(source_name)
            lines = [f"Schema for '{source_name}' ({schema.row_count} rows):"]
            for col in schema.columns:
                flags = []
                if col.is_datetime:
                    flags.append("datetime")
                if col.is_numeric:
                    flags.append("numeric")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                samples = f" (e.g., {', '.join(col.sample_values[:2])})" if col.sample_values else ""
                lines.append(f"  - {col.name}: {col.dtype}{flag_str}{samples}")
            return "\n".join(lines)
        except Exception as e:
            return f"Error getting schema: {e}"
    
    @agent.tool
    async def fetch_data(
        ctx: RunContext[AgentDeps],
        source_name: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> str:
        """
        Fetch data from a data source.
        
        Args:
            source_name: Name of the data source
            columns: List of specific columns to fetch (None = all columns)
            limit: Maximum number of rows to return
            order_by: Column to sort by (prefix with '-' for descending)
        """
        try:
            result = await ctx.deps.fetch_tool.fetch_data(
                source_name=source_name,
                columns=columns,
                limit=limit,
                order_by=order_by
            )
            
            # Format as a readable table
            if not result.data:
                return f"No data found in '{source_name}'"
            
            # Show first few rows as formatted text
            preview = result.data[:20]  # Cap at 20 rows for context window
            header = " | ".join(result.columns)
            separator = "-" * len(header)
            rows = []
            for record in preview:
                row = " | ".join(str(record.get(col, "")) for col in result.columns)
                rows.append(row)
            
            output = f"Data from '{source_name}' ({result.row_count} total rows, showing {len(preview)}):\n"
            output += f"{header}\n{separator}\n"
            output += "\n".join(rows)
            
            return output
        except Exception as e:
            return f"Error fetching data: {e}"
    
    @agent.tool
    async def create_chart(
        ctx: RunContext[AgentDeps],
        source_name: str,
        chart_type: str,
        x_column: str,
        y_columns: List[str],
        title: str = "",
        limit: Optional[int] = None
    ) -> str:
        """
        Create a chart from a data source.
        
        Args:
            source_name: Name of the data source
            chart_type: Type of chart ('line', 'bar', 'scatter', 'area')
            x_column: Column for x-axis (usually a date/time column)
            y_columns: Column(s) for y-axis
            title: Chart title
            limit: Limit data points (None = all)
        """
        try:
            # Fetch the data first
            result = await ctx.deps.fetch_tool.fetch_data(
                source_name=source_name,
                limit=limit,
                order_by=x_column  # Sort by x-axis for time series
            )
            
            if not result.data:
                return "No data available to chart"
            
            # Generate the chart
            chart_result = ctx.deps.chart_tool.create_chart(
                data=result.data,
                chart_type=chart_type,
                x_column=x_column,
                y_columns=y_columns,
                title=title
            )
            
            return (
                f"Chart created: '{chart_result.title}' "
                f"({chart_result.chart_type}, {chart_result.data_points} data points). "
                f"Chart image is available in the response."
            )
        except Exception as e:
            return f"Error creating chart: {e}"
    
    return agent


SYSTEM_PROMPT = """You are a data analysis assistant. You help users explore, analyze, and visualize data from various sources.

Your workflow:
1. First, check what data sources are available using list_data_sources
2. Get the schema of relevant sources using get_data_schema to understand the columns
3. Fetch data as needed using fetch_data
4. Create charts when the user wants to visualize data using create_chart

Guidelines:
- Always check the schema before fetching data so you know what columns exist
- For time-series data, identify the datetime column for the x-axis
- Default to line charts for time-series data
- When creating charts, use descriptive titles
- If the user's request is ambiguous, ask for clarification
- Summarize key findings from the data in your responses
- Keep data previews concise (use limits when appropriate)

Chart types available:
- line: Best for time-series and trends
- bar: Best for comparisons and categories
- scatter: Best for correlations and distributions
- area: Best for cumulative or stacked data
"""
