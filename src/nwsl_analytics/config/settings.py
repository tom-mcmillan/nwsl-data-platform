"""Configuration management for NWSL Analytics."""

import os
from pathlib import Path
from typing import List, Optional

from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Google Cloud Configuration
    gcp_project_id: str
    bigquery_dataset_id: str = "nwsl_analytics"
    google_application_credentials: Optional[str] = None
    
    # Data Configuration
    nwsl_seasons: List[str] = ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
    cache_dir: str = "/tmp/nwsl_cache"
    min_minutes_threshold: int = 450
    
    # MCP Server Configuration
    mcp_server_host: str = "localhost"
    mcp_server_port: int = 8000
    
    # Logging
    log_level: str = "INFO"
    log_file: str = "logs/nwsl_analytics.log"
    
    # Development
    debug: bool = False
    environment: str = "development"
    
    @validator("nwsl_seasons", pre=True)
    def parse_seasons(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",")]
        return v
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
