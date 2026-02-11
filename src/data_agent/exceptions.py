"""Custom exception hierarchy for the Data Agent."""


class DataAgentError(Exception):
    """Base exception for all Data Agent errors."""

    def __init__(self, message: str = "", detail: str = ""):
        self.message = message
        self.detail = detail
        super().__init__(message)


class SourceNotFoundError(DataAgentError):
    """Raised when a requested data source does not exist."""

    def __init__(self, source_name: str, available: list[str] | None = None):
        self.source_name = source_name
        self.available = available or []
        avail_str = ", ".join(self.available) if self.available else "none"
        super().__init__(
            message=f"Data source '{source_name}' not found. Available: {avail_str}",
            detail=source_name,
        )


class SourceValidationError(DataAgentError):
    """Raised when a data source configuration is invalid."""


class FetchError(DataAgentError):
    """Raised when fetching data from a source fails."""


class ChartError(DataAgentError):
    """Raised when chart generation fails."""


class ConfigError(DataAgentError):
    """Raised when configuration is invalid or missing."""
