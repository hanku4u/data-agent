"""Configuration management for the Data Agent using pydantic-settings."""

from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMConfig(BaseSettings):
    """LLM provider configuration."""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    provider: str = Field(default="openai", alias="LLM_PROVIDER")

    # OpenAI
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")

    # Anthropic
    anthropic_api_key: Optional[str] = Field(default=None, alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-sonnet-4-5-20250514", alias="ANTHROPIC_MODEL")

    # Ollama
    ollama_base_url: str = Field(default="http://localhost:11434", alias="OLLAMA_BASE_URL")
    ollama_model: str = Field(default="qwen3:8b", alias="OLLAMA_MODEL")


class AppConfig(BaseSettings):
    """Application configuration."""

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")

    llm: LLMConfig = Field(default_factory=LLMConfig)
    chart_output_dir: str = Field(default="./output/charts", alias="CHART_OUTPUT_DIR")
    data_dir: str = Field(default="./data", alias="DATA_DIR")
    sources_config_path: str = Field(default="./sources.yaml", alias="SOURCES_CONFIG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=True, alias="LOG_JSON")


def get_config() -> AppConfig:
    """Get application configuration."""
    return AppConfig()
