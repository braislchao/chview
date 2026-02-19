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

### Installation

```bash
# Clone the repository
git clone https://github.com/braislchao/chview.git
cd chview

# Install in development mode
pip install -e ".[dev,demo]"

# Install pre-commit hooks
pre-commit install
```

### Configuration

Create a `.env` file in the project root:

```env
CLICKHOUSE_HOST=your-host.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default
CLICKHOUSE_SECURE=True
```

### Run

```bash
# Using Make
make run

# Or directly
streamlit run src/chview/app.py
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
make install    # Install dependencies
make test       # Run tests with coverage
make lint       # Run linters (ruff, mypy)
make format     # Format code
make run        # Run Streamlit app
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

Run with synthetic data without a ClickHouse connection:

```bash
# Coming in Phase 2
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.
