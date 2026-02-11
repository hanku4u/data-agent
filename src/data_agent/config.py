"""Configuration management for the Data Agent."""

import os
from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()


class LLMConfig(BaseModel):
    """LLM provider configuration."""
    
    provider: str = os.getenv("LLM_PROVIDER", "openai")
    
    # OpenAI
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
    # Anthropic
    anthropic_api_key: Optional[str] = os.getenv("ANTHROPIC_API_KEY")
    anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250514")
    
    # Ollama
    ollama_base_url: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "qwen3:8b")


class AppConfig(BaseModel):
    """Application configuration."""
    
    llm: LLMConfig = LLMConfig()
    chart_output_dir: str = os.getenv("CHART_OUTPUT_DIR", "./output/charts")
    data_dir: str = os.getenv("DATA_DIR", "./data")
    sources_config_path: str = os.getenv("SOURCES_CONFIG", "./sources.yaml")


def get_config() -> AppConfig:
    """Get application configuration."""
    return AppConfig()
