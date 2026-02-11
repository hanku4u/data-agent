"""CSV and JSON file data source."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import DataSource
from ..models import DataSchema


class CSVSource(DataSource):
    """
    Data source for CSV and JSON files.
    
    Config options:
        path: Path to the file
        delimiter: CSV delimiter (default: ',')
        encoding: File encoding (default: 'utf-8')
        parse_dates: List of columns to parse as dates (auto-detected if not set)
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.file_path = Path(config["path"])
        self.delimiter = config.get("delimiter", ",")
        self.encoding = config.get("encoding", "utf-8")
        self.parse_dates = config.get("parse_dates", None)
        self._df_cache: Optional[pd.DataFrame] = None
    
    async def _load(self) -> pd.DataFrame:
        """Load the file into a DataFrame with caching."""
        if self._df_cache is not None:
            return self._df_cache
        
        suffix = self.file_path.suffix.lower()
        
        if suffix == ".json":
            df = pd.read_json(self.file_path, encoding=self.encoding)
        elif suffix in (".csv", ".tsv"):
            df = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                parse_dates=self.parse_dates if self.parse_dates else True,
                infer_datetime_format=True
            )
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
        
        self._df_cache = df
        return df
    
    async def fetch(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch data from CSV/JSON file."""
        df = await self._load()
        
        # Apply column selection
        if columns:
            df = df[columns]
        
        # Apply filters
        if filters:
            for col, value in filters.items():
                if col in df.columns:
                    if isinstance(value, dict):
                        # Support operators: {"gte": 10, "lte": 100}
                        if "gte" in value:
                            df = df[df[col] >= value["gte"]]
                        if "lte" in value:
                            df = df[df[col] <= value["lte"]]
                        if "gt" in value:
                            df = df[df[col] > value["gt"]]
                        if "lt" in value:
                            df = df[df[col] < value["lt"]]
                        if "eq" in value:
                            df = df[df[col] == value["eq"]]
                        if "contains" in value:
                            df = df[df[col].astype(str).str.contains(value["contains"], case=False)]
                    else:
                        df = df[df[col] == value]
        
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
        """Get schema from CSV/JSON file."""
        df = await self._load()
        
        return DataSchema(
            source_name=self.name,
            columns=self._detect_column_info(df),
            row_count=len(df)
        )
