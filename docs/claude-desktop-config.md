# Claude Desktop Configuration for NWSL Analytics MCP Server

## Overview

This document explains how to configure Claude Desktop to connect to your NWSL Analytics MCP server for professional soccer analytics.

## Configuration Options

### 🤖 **OpenAI Playground**
Use this URL directly in the MCP server configuration:
```
https://nwsl-analytics-mcp-havwlplupa-uc.a.run.app/mcp
```

### 🖥️ **Claude Desktop**
Add this configuration to your `claude_desktop_config.json` file:

**Location of config file:**
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

**Configuration:**

```json
{
  "mcpServers": {
    "nwsl-analytics": {
      "command": "curl",
      "args": [
        "-X", "POST",
        "https://nwsl-analytics-mcp-havwlplupa-uc.a.run.app/mcp",
        "-H", "Content-Type: application/json",
        "-d", "@-"
      ],
      "transport": "http"
    }
  }
}
```

## Alternative Local Configuration

If you want to run the server locally:

```json
{
  "mcpServers": {
    "nwsl-analytics-local": {
      "command": "python",
      "args": [
        "-m", "uvicorn",
        "src.nwsl_analytics.mcp.http_server:app",
        "--host", "127.0.0.1",
        "--port", "8001"
      ],
      "cwd": "/absolute/path/to/nwsl-data-platform"
    }
  }
}
```

## Server Capabilities

Once configured, Claude Desktop will have access to:

### 🔧 **Tools**
- `get_raw_data` - Access to comprehensive NWSL data including:
  - `squad_stats` - Team-level statistics
  - `player_stats` - Player performance data
  - `games` - Raw match data
  - `team_info` - Team information
  - `fbref_team_stats` - Professional FBref team stats (xG, possession, etc.)
  - `fbref_player_stats` - Professional FBref player stats
  - `fbref_matches` - Professional match data
  - `fbref_player_match_stats` - Detailed player match statistics

### 📄 **Resources**
- `nwsl://seasons` - Available seasons
- `nwsl://teams/2024` - Team lists
- `nwsl://stats/summary/2024` - Season summaries
- `nwsl://standings/2024` - Current standings

### ✨ **Prompts**
- `analyze-team-performance` - Comprehensive team analysis
- `compare-teams` - Side-by-side team comparisons
- `season-recap` - Professional season reviews

## Usage Examples

After configuration, you can ask Claude:

### Using Tools
- "Get FBref team stats for the 2024 NWSL season"
- "Show me player stats for Kansas City Current in 2024"
- "Get raw match data for the 2024 season with limit 20"
- "Retrieve squad stats for all teams in 2024"

### Using Resources
- "What NWSL teams are in the 2024 season?"
- "Show me the season summary for 2024"

### Using Prompts
- "Use the analyze-team-performance prompt for Kansas City Current in 2024"
- "Compare Orlando Pride and Washington Spirit using the compare-teams prompt"

## Data Available

### **Data Types & Sources**
- **FBref Professional Stats (2020-2025)**: Expected Goals (xG), possession, passing accuracy, defensive stats
- **Basic Match Data (2016-2024)**: Game results, scores, attendance
- **Team Information**: All 14 NWSL teams with metadata

### **Available Data Types for `get_raw_data` Tool**
1. **`fbref_team_stats`** - Professional team statistics with xG, possession
2. **`fbref_player_stats`** - Professional player statistics
3. **`fbref_matches`** - Professional match data
4. **`fbref_player_match_stats`** - Detailed player match performance
5. **`squad_stats`** - Aggregated team statistics
6. **`player_stats`** - Basic player performance data
7. **`games`** - Raw match results and details
8. **`team_info`** - Team metadata and information

### **Seasons & Coverage**
- **2020-2025**: Full FBref professional statistics
- **2016-2024**: Basic match data
- **All 14 NWSL teams**: Complete coverage

## Troubleshooting

1. **Server Not Found**: Ensure the URL is accessible: https://nwsl-analytics-mcp-havwlplupa-uc.a.run.app/mcp
2. **No MCP Elements**: Restart Claude Desktop after saving the config
3. **Local Server Issues**: Check Python environment and BigQuery credentials

## Testing

Test the server directly:

```bash
curl -X POST https://nwsl-analytics-mcp-havwlplupa-uc.a.run.app/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```