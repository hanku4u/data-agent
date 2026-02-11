"""Pydantic models for request/response schemas."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field


# --- Data Source Models ---

class DataSourceType(str, Enum):
    """Supported data source types."""
    CSV = "csv"
    JSON = "json"
    REST_API = "rest_api"
    SQL = "sql"


class DataSourceConfig(BaseModel):
    """Configuration for a data source."""
    name: str
    type: DataSourceType
    config: Dict[str, Any] = Field(default_factory=dict)
    description: str = ""


class DataSourceInfo(BaseModel):
    """Information about a registered data source."""
    name: str
    type: DataSourceType
    description: str = ""
    columns: List[str] = Field(default_factory=list)
    row_count: Optional[int] = None


# --- Data Models ---

class DataResult(BaseModel):
    """Result from a data fetch operation."""
    source_name: str
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ColumnInfo(BaseModel):
    """Information about a data column."""
    name: str
    dtype: str
    sample_values: List[Any] = Field(default_factory=list)
    is_datetime: bool = False
    is_numeric: bool = False


class DataSchema(BaseModel):
    """Schema information for a data source."""
    source_name: str
    columns: List[ColumnInfo]
    row_count: int


# --- Chart Models ---

class ChartType(str, Enum):
    """Supported chart types."""
    LINE = "line"
    BAR = "bar"
    SCATTER = "scatter"
    AREA = "area"


class ChartRequest(BaseModel):
    """Request to generate a chart."""
    data_source: str
    chart_type: ChartType = ChartType.LINE
    x_column: str
    y_columns: List[str]
    title: str = ""
    x_label: str = ""
    y_label: str = ""
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None


class ChartResult(BaseModel):
    """Result of chart generation."""
    chart_type: str
    title: str
    image_base64: Optional[str] = None
    html: Optional[str] = None
    data_points: int = 0


# --- API Models ---

class AgentQueryRequest(BaseModel):
    """Request to query the agent."""
    query: str = Field(..., description="Natural language query about data")
    data_source: Optional[str] = Field(None, description="Specific data source to query")


class AgentQueryResponse(BaseModel):
    """Response from the agent."""
    answer: str
    data: Optional[DataResult] = None
    chart: Optional[ChartResult] = None
    sources_used: List[str] = Field(default_factory=list)
    tools_used: List[str] = Field(default_factory=list)
