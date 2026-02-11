# Data Agent 🤖📊

Pluggable AI agent for data fetching, transformation, and chart generation. Built with [Pydantic AI](https://github.com/pydantic/pydantic-ai), FastAPI, and Plotly.

[![CI](https://github.com/hanku4u/data-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/hanku4u/data-agent/actions/workflows/ci.yml)

## Features

- **Multi-source data fetching** — CSV, JSON, SQL databases, REST APIs
- **AI-powered queries** — Natural language data exploration via Pydantic AI agents
- **Chart generation** — Line, bar, scatter, and area charts with Plotly
- **Data transformations** — GroupBy, resample, rolling averages, aggregations
- **Structured logging** — JSON-formatted logs with structlog
- **Error handling** — Custom exception hierarchy with proper HTTP status mapping
- **Source validation** — Validates data source configs before registration
- **SQL injection protection** — Parameterized queries and column whitelisting

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Configure (copy and edit)
cp .env.example .env

# Run the API server
uvicorn data_agent.api:app --reload
```

## Configuration

Set environment variables or use a `.env` file:

```env
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=qwen3:8b
SOURCES_CONFIG=./sources.yaml
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/agent/query` | Query the AI agent |
| `GET` | `/data-sources` | List registered sources |
| `POST` | `/data-sources` | Register a new source |
| `GET` | `/data-sources/{name}/schema` | Get source schema |
| `DELETE` | `/data-sources/{name}` | Remove a source |
| `POST` | `/agent/chart` | Generate a chart directly |

## Usage Examples

### Register a CSV source

```bash
curl -X POST http://localhost:8000/data-sources \
  -H "Content-Type: application/json" \
  -d '{"name": "metrics", "type": "csv", "config": {"path": "./data/sample_metrics.csv"}}'
```

### Query the agent

```bash
curl -X POST http://localhost:8000/agent/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me a line chart of cpu_usage over time from the metrics source"}'
```

### YAML source configuration

Define sources in `sources.yaml`:

```yaml
sources:
  sales:
    type: csv
    config:
      path: ./data/sales.csv
    description: Monthly sales data

  analytics_db:
    type: sql
    config:
      connection_string: sqlite:///./data/analytics.db
      table: events
    description: Analytics events
```

## Data Transformations

The agent can perform data transformations before charting:

- **groupby** — Group by columns with sum/mean/count/min/max
- **resample** — Resample time-series (D/W/M/Q/Y frequencies)
- **rolling_average** — Moving averages with configurable window
- **aggregate** — Single-value computations (sum, mean, std, etc.)

## Architecture

```
src/data_agent/
├── agent.py          # Pydantic AI agent with tools
├── api.py            # FastAPI application
├── config.py         # Pydantic Settings configuration
├── dependencies.py   # FastAPI dependency injection
├── exceptions.py     # Custom exception hierarchy
├── log.py            # Structured logging (structlog)
├── middleware.py      # Error handler & request logging
├── models.py         # Pydantic request/response models
├── registry.py       # Source registry
├── charts/
│   └── engine.py     # Plotly chart generation
├── sources/
│   ├── base.py       # Abstract DataSource
│   ├── csv_source.py # CSV/JSON file source
│   ├── sql_source.py # SQL database source
│   └── api_source.py # REST API source
└── tools/
    ├── fetch.py      # Data fetch tool
    ├── chart.py      # Chart generation tool
    └── transform.py  # Data transformation tool
```

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific phase
python -m pytest tests/test_phase0.py -v
```

## License

MIT
