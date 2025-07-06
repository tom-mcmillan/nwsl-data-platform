# NWSL Analytics Platform

Professional soccer analytics platform for NWSL data discovery and insights.

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Google Cloud Platform account with BigQuery enabled
- Git

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd nwsl-analytics

# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
make install-dev

# Copy environment template
cp .env.example .env
# Edit .env with your configuration
```

## Usage

```bash
# Ingest NWSL data
make ingest

# Start MCP server
make server

# Run analysis
make analyze

# Run tests
make test
```

## Features

- Data Ingestion: Automated NWSL data collection from FBref
- Analytics Engine: Player, team, and league analysis tools
- Pattern Discovery: ML-powered insight discovery
- MCP Integration: Model Context Protocol server for AI integration
- Professional Tooling: Linting, testing, and CI/CD ready

## Architecture

src/nwsl_analytics/
├── config/          # Configuration management
├── data/            # Data ingestion and models
├── analytics/       # Analysis engines
├── mcp/             # MCP server and tools
└── utils/           # Utilities and helpers

## Development

```bash
# Format code
make format

# Run linting
make lint

# Run tests with coverage
make test-cov

# Clean up
make clean
```

## Documentation
See the docs/ directory for detailed documentation.


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License
MIT License - see LICENSE file for details.
EOF
