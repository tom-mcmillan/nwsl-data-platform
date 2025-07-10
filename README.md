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

- **Data Ingestion**: Automated NWSL data collection from FBref API
- **Raw Data Access**: Direct access to 8 different data types via MCP protocol
- **Professional Statistics**: xG, possession, passing accuracy, defensive metrics
- **MCP Integration**: Streamlined Model Context Protocol server for AI integration
- **Comprehensive Coverage**: 2020-2025 FBref data + 2016-2024 basic match data
- **Professional Tooling**: Linting, testing, and CI/CD ready

## Architecture

```
nwsl-data-platform/
├── data/                    # All data files organized by processing stage
│   ├── raw/excel/          # Your Excel player stats files
│   ├── processed/          # Clean CSV files ready for BigQuery  
│   └── external/           # Third-party data sources
├── analytics/              # Advanced soccer analytics modules
│   ├── expected_goals/     # xG calculation and analysis
│   ├── replacement_value/  # Player WAR calculations
│   ├── shot_quality/       # Shot profiling and analysis
│   ├── correlations/       # Statistical pattern discovery
│   └── win_expectancy/     # Match outcome probability
├── scripts/
│   ├── ingestion/          # Data processing scripts
│   ├── deployment/         # BigQuery upload scripts
│   └── maintenance/        # Utility scripts
├── src/nwsl_analytics/     # Core platform code
│   ├── config/            # Configuration management
│   ├── data/              # Data models and ingestion
│   ├── mcp/               # MCP server and tools
│   └── utils/             # Utilities and helpers
└── tests/                 # Test suites
```

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
