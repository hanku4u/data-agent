"""Data fetch tool for the agent.

DEPRECATED: This module is deprecated. Use SourceRegistry directly instead.
FetchTool is kept for backward compatibility only.
"""

import warnings
from typing import Any, Dict, List, Optional

from ..sources import DataSource, CSVSource, APISource, SQLSource
from ..models import DataSourceConfig, DataSourceType, DataResult, DataSchema


class FetchTool:
    """
    Tool for fetching data from registered data sources.
    
    Manages a registry of data sources and provides methods
    for the agent to discover and query them.
    """
    
    # Map source types to their implementations
    SOURCE_CLASSES = {
        DataSourceType.CSV: CSVSource,
        DataSourceType.JSON: CSVSource,  # CSVSource handles both
        DataSourceType.REST_API: APISource,
        DataSourceType.SQL: SQLSource,
    }
    
    def __init__(self) -> None:
        """Initialize the fetch tool with an empty source registry.
        
        DEPRECATED: Use SourceRegistry directly instead.
        """
        warnings.warn(
            "FetchTool is deprecated. Use SourceRegistry directly instead.",
            DeprecationWarning,
            stacklevel=2
        )
        self._sources: Dict[str, DataSource] = {}
    
    def register_source(self, config: DataSourceConfig) -> str:
        """
        Register a new data source.
        
        Args:
            config: Data source configuration
        
        Returns:
            Confirmation message
        """
        source_class = self.SOURCE_CLASSES.get(config.type)
        if not source_class:
            raise ValueError(f"Unsupported source type: {config.type}")
        
        self._sources[config.name] = source_class(
            name=config.name,
            config=config.config
        )
        
        return f"Data source '{config.name}' registered ({config.type.value})"
    
    def unregister_source(self, name: str) -> str:
        """Remove a data source from the registry."""
        if name in self._sources:
            del self._sources[name]
            return f"Data source '{name}' removed"
        return f"Data source '{name}' not found"
    
    def list_sources(self) -> List[str]:
        """List all registered data source names."""
        return list(self._sources.keys())
    
    def get_source(self, name: str) -> DataSource:
        """Get a registered data source by name."""
        if name not in self._sources:
            available = ", ".join(self._sources.keys()) or "none"
            raise ValueError(f"Data source '{name}' not found. Available: {available}")
        return self._sources[name]
    
    async def fetch_data(
        self,
        source_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> DataResult:
        """
        Fetch data from a named source.
        
        Args:
            source_name: Name of the registered data source
            columns: Specific columns to retrieve
            filters: Filters to apply
            limit: Maximum rows to return
            order_by: Column to sort by (prefix with '-' for descending)
        
        Returns:
            DataResult with fetched data
        """
        source = self.get_source(source_name)
        return await source.fetch_as_result(
            columns=columns,
            filters=filters,
            limit=limit,
            order_by=order_by
        )
    
    async def get_schema(self, source_name: str) -> DataSchema:
        """
        Get schema information for a data source.
        
        Args:
            source_name: Name of the registered data source
        
        Returns:
            DataSchema with column info
        """
        source = self.get_source(source_name)
        return await source.get_schema()
