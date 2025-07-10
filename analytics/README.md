# Analytics Modules

This directory contains advanced soccer analytics tools for NWSL data analysis.

## Modules

### Core Analytics Tools

- **`expected_goals/`** - xG calculation and shot quality analysis
  - Player and team expected goals models
  - Shot location and context analysis
  - xG vs actual goal performance

- **`replacement_value/`** - Player value above replacement level
  - Wins Above Replacement (WAR) for soccer
  - Position-specific baseline calculations
  - Player impact metrics

- **`shot_quality/`** - Shot analysis and profiling
  - Shot zone effectiveness
  - Assist type impact on shot quality
  - Goalkeeper positioning analysis

- **`correlations/`** - Statistical pattern discovery
  - Performance metric relationships
  - Predictive indicators for success
  - League-wide trend analysis

- **`win_expectancy/`** - Match outcome probability
  - Live win probability models
  - Situational leverage analysis
  - Expected points calculations

## Implementation Plan

Each module will contain:
- Core calculation functions
- BigQuery integration
- MCP tool interfaces
- Validation and testing

## Data Sources

- Team-level: FBref professional statistics
- Player-level: Excel exports with xG, goals, assists, minutes
- Match-level: Game results and context