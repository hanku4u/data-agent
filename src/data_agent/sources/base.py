"""Abstract base class for data sources."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd

from ..models import DataResult, DataSchema, ColumnInfo


class DataSource(ABC):
    """
    Abstract base class for all data sources.
    
    Implement this to add new data source types (e.g., GraphQL, S3, etc.).
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize a data source.
        
        Args:
            name: Unique identifier for this data source
            config: Source-specific configuration
        """
        self.name = name
        self.config = config
    
    @abstractmethod
    async def fetch(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch data from the source.
        
        Args:
            columns: Specific columns to fetch (None = all)
            filters: Key-value filters to apply
            limit: Maximum rows to return
            order_by: Column to sort by
        
        Returns:
            DataFrame with the fetched data
        """
        ...
    
    @abstractmethod
    async def get_schema(self) -> DataSchema:
        """
        Get schema information about this data source.
        
        Returns:
            DataSchema with column names, types, and sample values
        """
        ...
    
    async def fetch_as_result(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> DataResult:
        """
        Fetch data and return as a DataResult model.
        
        Convenience method that wraps fetch() output in a DataResult.
        """
        df = await self.fetch(columns=columns, filters=filters, limit=limit, order_by=order_by)
        
        return DataResult(
            source_name=self.name,
            columns=list(df.columns),
            data=df.to_dict(orient="records"),
            row_count=len(df),
            metadata={
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
        )
    
    @staticmethod
    def _detect_column_info(df: pd.DataFrame) -> List[ColumnInfo]:
        """Detect column information from a DataFrame."""
        columns = []
        for col in df.columns:
            is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
            is_numeric = pd.api.types.is_numeric_dtype(df[col])
            
            # Try to detect datetime strings
            if not is_datetime and df[col].dtype == object:
                try:
                    pd.to_datetime(df[col].head(5))
                    is_datetime = True
                except (ValueError, TypeError):
                    pass
            
            sample = df[col].dropna().head(3).tolist()
            
            columns.append(ColumnInfo(
                name=col,
                dtype=str(df[col].dtype),
                sample_values=[str(v) for v in sample],
                is_datetime=is_datetime,
                is_numeric=is_numeric
            ))
        
        return columns
