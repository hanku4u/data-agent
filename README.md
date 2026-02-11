# Data Agent 📊

A pluggable AI agent built with **Pydantic AI** that fetches data from various sources and generates charts. Exposed via **FastAPI** for easy integration.

## Features

- 🤖 **Pydantic AI Agent** — Natural language data queries with pluggable LLM (OpenAI, Anthropic, Ollama)
- 🔌 **Data Source Abstraction** — Plug in CSV, JSON, REST APIs, SQL databases, and more
- 📈 **Chart Generation** — Line, bar, scatter, area charts with automatic time-series detection
- 🚀 **FastAPI Endpoint** — Query the agent via REST API
- 🧩 **Extensible** — Add new data sources and chart types easily

## Architecture

```
FastAPI → Pydantic AI Agent → Tools
                                ├── Fetch Tool (Data Source Abstraction)
                                │     ├── CSV/JSON files
                                │     ├── REST APIs
                                │     ├── SQL databases
                                │     └── (extensible)
                                └── Chart Tool (Plotly)
                                      ├── Line chart
                                      ├── Bar chart
                                      ├── Scatter plot
                                      └── Area chart
```

## Quick Start

```bash
# Clone
git clone https://github.com/hanku4u/data-agent.git
cd data-agent

# Install
pip install -e .

# Configure
cp .env.example .env
# Edit .env with your LLM provider settings

# Run
uvicorn data_agent.api:app --reload
```

## API Endpoints

### Query the Agent
```bash
POST /agent/query
{
  "query": "Show me a line chart of daily temperatures from weather.csv",
  "data_source": "weather_csv"
}
```

### Generate a Chart
```bash
POST /agent/chart
{
  "data_source": "weather_csv",
  "chart_type": "line",
  "x_column": "date",
  "y_columns": ["temperature"],
  "title": "Daily Temperatures"
}
```

### List Data Sources
```bash
GET /data-sources
```

### Register Data Source
```bash
POST /data-sources
{
  "name": "weather_csv",
  "type": "csv",
  "config": {
    "path": "/path/to/weather.csv"
  }
}
```

## Configuration

### LLM Providers

```env
# OpenAI
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...

# Anthropic
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...

# Ollama (local)
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://192.168.4.210:11434
OLLAMA_MODEL=qwen3:8b
```

### Data Sources

Data sources are configured via the API or a `sources.yaml` file:

```yaml
sources:
  weather_csv:
    type: csv
    config:
      path: ./data/weather.csv

  sales_api:
    type: rest_api
    config:
      url: https://api.example.com/sales
      headers:
        Authorization: "Bearer ${API_TOKEN}"

  metrics_db:
    type: sql
    config:
      connection_string: "sqlite:///./data/metrics.db"
      table: "metrics"
```

## Project Structure

```
data-agent/
├── src/data_agent/
│   ├── __init__.py
│   ├── agent.py          # Pydantic AI agent definition
│   ├── api.py            # FastAPI application
│   ├── config.py         # Configuration management
│   ├── models.py         # Pydantic models (request/response)
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── fetch.py      # Data fetch tool
│   │   └── chart.py      # Chart generation tool
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── base.py       # DataSource abstract base
│   │   ├── csv_source.py # CSV/JSON file source
│   │   ├── api_source.py # REST API source
│   │   └── sql_source.py # SQL database source
│   └── charts/
│       ├── __init__.py
│       └── engine.py     # Plotly chart engine
├── tests/
├── data/                 # Sample data files
├── pyproject.toml
├── .env.example
└── README.md
```

## Tech Stack

- **[Pydantic AI](https://ai.pydantic.dev/)** — Agent framework with type safety
- **[FastAPI](https://fastapi.tiangolo.com/)** — API layer
- **[Plotly](https://plotly.com/python/)** — Chart generation
- **[Pandas](https://pandas.pydata.org/)** — Data manipulation
- **[SQLAlchemy](https://www.sqlalchemy.org/)** — SQL database support

## License

MIT

## Author

Created by [@hanku4u](https://github.com/hanku4u) with AI assistance from RockLobster 🦞
