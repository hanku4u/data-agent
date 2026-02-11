"""SQL database data source with injection protection."""

import re
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text, inspect

from .base import DataSource
from ..models import DataSchema, ColumnInfo

# Only allow simple identifiers (letters, digits, underscores)
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe SQL identifier."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


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
        self._valid_columns: Optional[set[str]] = None

    def _get_valid_columns(self) -> set[str]:
        """Get the set of valid column names for the table."""
        if self._valid_columns is None and self.table:
            inspector = inspect(self.engine)
            cols = inspector.get_columns(self.table)
            self._valid_columns = {c["name"] for c in cols}
        return self._valid_columns or set()

    def _validate_column(self, col: str) -> str:
        """Validate a column name against the table schema."""
        _validate_identifier(col)
        valid = self._get_valid_columns()
        if valid and col not in valid:
            raise ValueError(f"Column '{col}' not found in table '{self.table}'. Valid: {sorted(valid)}")
        return col

    async def fetch(
        self,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch data from SQL database with injection protection."""
        if self.default_query:
            query = self.default_query
            params: dict[str, Any] = {}
        elif self.table:
            _validate_identifier(self.table)

            if columns:
                for c in columns:
                    self._validate_column(c)
                col_str = ", ".join(columns)
            else:
                col_str = "*"

            query = f"SELECT {col_str} FROM {self.table}"
            params = {}

            # Apply filters with parameterized queries
            if filters:
                conditions = []
                for i, (col, value) in enumerate(filters.items()):
                    self._validate_column(col)
                    if isinstance(value, dict):
                        for op, val in value.items():
                            param_name = f"p{i}_{op}"
                            sql_op = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "="}.get(op)
                            if sql_op:
                                conditions.append(f"{col} {sql_op} :{param_name}")
                                params[param_name] = val
                    else:
                        param_name = f"p{i}"
                        conditions.append(f"{col} = :{param_name}")
                        params[param_name] = value

                if conditions:
                    query += " WHERE " + " AND ".join(conditions)

            # Apply sorting with validation
            if order_by:
                direction = "ASC"
                sort_col = order_by
                if sort_col.startswith("-"):
                    sort_col = sort_col[1:]
                    direction = "DESC"
                self._validate_column(sort_col)
                query += f" ORDER BY {sort_col} {direction}"

            # Apply limit
            if limit:
                query += f" LIMIT {int(limit)}"
        else:
            raise ValueError("Either 'table' or 'query' must be configured")

        with self.engine.connect() as conn:
            if params:
                df = pd.read_sql(text(query), conn, params=params)
            else:
                df = pd.read_sql(text(query), conn)

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

            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table}"))
                row_count = result.scalar()

            columns = [
                ColumnInfo(
                    name=col["name"],
                    dtype=str(col["type"]),
                    is_datetime="date" in str(col["type"]).lower()
                    or "time" in str(col["type"]).lower(),
                    is_numeric=any(
                        t in str(col["type"]).lower()
                        for t in ["int", "float", "decimal", "numeric", "real"]
                    ),
                )
                for col in sql_columns
            ]

            return DataSchema(
                source_name=self.name,
                columns=columns,
                row_count=row_count,
            )
        else:
            df = await self.fetch(limit=10)
            return DataSchema(
                source_name=self.name,
                columns=self._detect_column_info(df),
                row_count=len(df),
            )
