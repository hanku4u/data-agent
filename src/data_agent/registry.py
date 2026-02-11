"""Source registry for managing data sources."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .sources.base import DataSource
from .sources.csv_source import CSVSource
from .sources.api_source import APISource
from .sources.sql_source import SQLSource
from .models import DataSourceConfig, DataSourceType, DataResult, DataSchema
from .exceptions import SourceNotFoundError, SourceValidationError


class SourceRegistry:
    """Registry of data sources keyed by name."""

    SOURCE_CLASSES: dict[DataSourceType, type[DataSource]] = {
        DataSourceType.CSV: CSVSource,
        DataSourceType.JSON: CSVSource,
        DataSourceType.REST_API: APISource,
        DataSourceType.SQL: SQLSource,
    }

    def __init__(self) -> None:
        self._sources: Dict[str, DataSource] = {}

    def register(self, config: DataSourceConfig) -> str:
        """Register a data source from config. Returns confirmation message."""
        source_class = self.SOURCE_CLASSES.get(config.type)
        if not source_class:
            raise SourceValidationError(f"Unsupported source type: {config.type}")

        source = source_class(name=config.name, config=config.config)
        source.validate()
        self._sources[config.name] = source
        return f"Data source '{config.name}' registered ({config.type.value})"

    def unregister(self, name: str) -> str:
        if name in self._sources:
            del self._sources[name]
            return f"Data source '{name}' removed"
        return f"Data source '{name}' not found"

    def list(self) -> List[str]:
        return list(self._sources.keys())

    def get(self, name: str) -> DataSource:
        if name not in self._sources:
            raise SourceNotFoundError(name, available=self.list())
        return self._sources[name]

    async def fetch_data(
        self,
        source_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> DataResult:
        source = self.get(source_name)
        return await source.fetch_as_result(
            columns=columns, filters=filters, limit=limit, order_by=order_by
        )

    async def get_schema(self, source_name: str) -> DataSchema:
        source = self.get(source_name)
        return await source.get_schema()
