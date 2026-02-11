"""REST API data source."""

from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from .base import DataSource
from ..models import DataSchema


class APISource(DataSource):
    """
    Data source for REST APIs.
    
    Config options:
        url: API endpoint URL
        method: HTTP method (default: 'GET')
        headers: Request headers
        params: Query parameters
        data_path: JSON path to data array (e.g., 'results' or 'data.items')
        auth_token: Bearer token (convenience)
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.url = config["url"]
        self.method = config.get("method", "GET").upper()
        self.headers = config.get("headers", {})
        self.params = config.get("params", {})
        self.data_path = config.get("data_path", None)
        
        # Convenience: add bearer token if provided
        auth_token = config.get("auth_token")
        if auth_token:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    def validate(self) -> None:
        """Validate API source configuration."""
        if not self.url:
            from ..exceptions import SourceValidationError
            raise SourceValidationError("API source requires 'url' in config")

    def _extract_data(self, response_json: Any) -> List[Dict[str, Any]]:
        """Extract data array from API response using data_path."""
        if self.data_path is None:
            # Assume response is a list or has a 'data' key
            if isinstance(response_json, list):
                return response_json
            elif isinstance(response_json, dict):
                if "data" in response_json:
                    return response_json["data"]
                if "results" in response_json:
                    return response_json["results"]
                return [response_json]
            return []
        
        # Navigate the JSON path (e.g., "data.items")
        data = response_json
        for key in self.data_path.split("."):
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return []
        
        return data if isinstance(data, list) else [data]
    
    async def fetch(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch data from REST API."""
        # Merge any filter params with base params
        params = {**self.params}
        if filters:
            params.update(filters)
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                params=params if self.method == "GET" else None,
                json=params if self.method == "POST" else None
            )
            response.raise_for_status()
        
        data = self._extract_data(response.json())
        df = pd.DataFrame(data)
        
        # Try to parse datetime columns
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_datetime(df[col])
                except (ValueError, TypeError):
                    pass
        
        # Apply column selection
        if columns:
            df = df[[c for c in columns if c in df.columns]]
        
        # Apply sorting
        if order_by:
            ascending = True
            if order_by.startswith("-"):
                order_by = order_by[1:]
                ascending = False
            if order_by in df.columns:
                df = df.sort_values(order_by, ascending=ascending)
        
        # Apply limit
        if limit:
            df = df.head(limit)
        
        return df
    
    async def get_schema(self) -> DataSchema:
        """Get schema by fetching a sample from the API."""
        df = await self.fetch(limit=10)
        
        return DataSchema(
            source_name=self.name,
            columns=self._detect_column_info(df),
            row_count=len(df)
        )
