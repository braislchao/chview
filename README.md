# CHView

**ClickHouse Data Lineage Visualization**

A modern web application for visualizing ClickHouse database schemas, materialized view lineages, and performance metrics.

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Features

- **Interactive Lineage Graphs**: Visualize materialized view dependencies with an interactive flow diagram
- **Real-time Metrics**: Monitor throughput, execution counts, and performance over time
- **Storage Analytics**: Analyze disk usage, compression ratios, and partition sizes
- **Schema Browser**: Navigate databases and tables with detailed metadata
- **Health Monitoring**: Track MV errors and Kafka consumer health

## Quick Start

### Option A — Full demo (app + local ClickHouse + live traffic)

Requires only [Docker](https://docs.docker.com/get-docker/). One command starts everything using a `demo` Compose profile:

```bash
git clone https://github.com/braislchao/chview.git
cd chview
make up-demo
```

This starts three containers:

| Container | What it does |
|---|---|
| `chview-clickhouse` | ClickHouse 24.8, seeded with the e-commerce demo schema on first boot |
| `chview-app` | CHView UI at <http://localhost:8501> |
| `chview-traffic` | Continuously inserts synthetic events, keeping all 9 MVs busy |

```bash
make logs          # follow all service logs
make logs-traffic  # follow traffic generator only
make down-demo     # stop (data preserved in Docker volume)
make reset         # stop + wipe data (fresh start)
```

### Option B — App only (BYO ClickHouse)

Already have a ClickHouse instance? Edit `.env.docker` with your credentials, then:

```bash
make up            # starts only the app container → http://localhost:8501
make down          # stop
```

Or run locally without Docker:

```bash
pip install -e ".[dev,demo]"
cp .env.example .env   # fill in your credentials
make run               # streamlit run src/chview/app.py
```

## Development

### Project Structure

```
chview/
├── src/chview/           # Main package
│   ├── core/            # Business logic
│   ├── db/              # Database layer
│   ├── pages/           # Streamlit pages
│   ├── components/      # UI components
│   └── lineage/         # Lineage engine
├── tests/               # Test suite
├── demo/                # Demo data
└── docs/                # Documentation
```

### Commands

```bash
make install       # Install dependencies
make test          # Run tests with coverage
make lint          # Run linters (ruff, mypy)
make format        # Format code
make run           # Run Streamlit app locally (needs .env)

# Docker
make up            # App only (BYO ClickHouse via .env.docker)
make up-demo       # App + ClickHouse + traffic generator
make down          # Stop app
make down-demo     # Stop full demo stack
make reset         # Stop + wipe data volume
make logs          # Follow all logs
make logs-traffic  # Follow traffic generator logs
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=chview --cov-report=html
```

## Requirements

- Python 3.9+
- ClickHouse 21.3+ (for `system.query_views_log`)
- Streamlit 1.37+

## Demo Mode

Two modes are available — see **Quick Start** above:

- `make up-demo` — full local stack (ClickHouse + app + live traffic), no cloud account needed
- `make up` — app only, point `.env.docker` at any ClickHouse instance

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.
