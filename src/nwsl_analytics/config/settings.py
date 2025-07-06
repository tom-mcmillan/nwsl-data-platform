"""Configuration management for NWSL Analytics."""

import os
from pathlib import Path
from typing import List, Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Google Cloud Configuration
    gcp_project_id: str
    bigquery_dataset_id: str = "nwsl_fbref"
    google_application_credentials: Optional[str] = None
    
    # Data Configuration
    nwsl_seasons: str = "2016,2017,2018,2019,2020,2021,2022,2023,2024"
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
    
    @property
    def seasons_list(self) -> List[str]:
        """Convert comma-separated seasons to list"""
        return [s.strip() for s in self.nwsl_seasons.split(",")]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields like FBREF_API_KEY


# Global settings instance
settings = Settings()
