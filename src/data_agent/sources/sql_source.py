"""SQL database data source."""

from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text, inspect

from .base import DataSource
from ..models import DataSchema, ColumnInfo


class SQLSource(DataSource):
    """
    Data source for SQL databases.
    
    Config options:
        connection_string: SQLAlchemy connection string
        table: Default table name
        query: Custom SQL query (overrides table)
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.connection_string = config["connection_string"]
        self.table = config.get("table")
        self.default_query = config.get("query")
        self.engine = create_engine(self.connection_string)
    
    async def fetch(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch data from SQL database."""
        if self.default_query:
            # Use custom query
            query = self.default_query
        elif self.table:
            # Build query from table
            col_str = ", ".join(columns) if columns else "*"
            query = f"SELECT {col_str} FROM {self.table}"
            
            # Apply filters
            if filters:
                conditions = []
                for col, value in filters.items():
                    if isinstance(value, dict):
                        if "gte" in value:
                            conditions.append(f"{col} >= {value['gte']}")
                        if "lte" in value:
                            conditions.append(f"{col} <= {value['lte']}")
                        if "gt" in value:
                            conditions.append(f"{col} > {value['gt']}")
                        if "lt" in value:
                            conditions.append(f"{col} < {value['lt']}")
                    else:
                        conditions.append(f"{col} = '{value}'")
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            
            # Apply sorting
            if order_by:
                direction = "ASC"
                if order_by.startswith("-"):
                    order_by = order_by[1:]
                    direction = "DESC"
                query += f" ORDER BY {order_by} {direction}"
            
            # Apply limit
            if limit:
                query += f" LIMIT {limit}"
        else:
            raise ValueError("Either 'table' or 'query' must be configured")
        
        df = pd.read_sql(query, self.engine)
        
        # Try to parse datetime columns
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_datetime(df[col])
                except (ValueError, TypeError):
                    pass
        
        return df
    
    async def get_schema(self) -> DataSchema:
        """Get schema from SQL database."""
        if self.table:
            inspector = inspect(self.engine)
            sql_columns = inspector.get_columns(self.table)
            
            # Get row count
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table}"))
                row_count = result.scalar()
            
            columns = [
                ColumnInfo(
                    name=col["name"],
                    dtype=str(col["type"]),
                    is_datetime="date" in str(col["type"]).lower() or "time" in str(col["type"]).lower(),
                    is_numeric=any(t in str(col["type"]).lower() for t in ["int", "float", "decimal", "numeric", "real"])
                )
                for col in sql_columns
            ]
            
            return DataSchema(
                source_name=self.name,
                columns=columns,
                row_count=row_count
            )
        else:
            # Fallback: execute query with limit
            df = await self.fetch(limit=10)
            return DataSchema(
                source_name=self.name,
                columns=self._detect_column_info(df),
                row_count=len(df)
            )
