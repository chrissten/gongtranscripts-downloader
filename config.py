"""
Configuration management for Gong Transcripts Downloader
"""
import os
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from pydantic import validator, Field
from pydantic_settings import BaseSettings


class GongConfig(BaseSettings):
    """Configuration settings for Gong API and transcript downloading."""
    
    # Gong API Credentials
    gong_access_key: str = Field(env='GONG_ACCESS_KEY')
    gong_access_key_secret: str = Field(env='GONG_ACCESS_KEY_SECRET')
    gong_subdomain: str = Field(env='GONG_SUBDOMAIN')
    
    # Download Configuration
    download_start_date: str = Field(default='2022-01-01', env='DOWNLOAD_START_DATE')
    download_end_date: str = Field(default='2024-12-31', env='DOWNLOAD_END_DATE')
    output_directory: str = Field(default='./transcripts', env='OUTPUT_DIRECTORY')
    max_concurrent_downloads: int = Field(default=3, env='MAX_CONCURRENT_DOWNLOADS')
    
    # API Configuration
    api_rate_limit: float = Field(default=2.5, env='API_RATE_LIMIT')  # calls per second
    api_timeout: int = Field(default=60, env='API_TIMEOUT')  # seconds
    max_retries: int = Field(default=3, env='MAX_RETRIES')
    
    # Output Configuration
    save_raw_json: bool = Field(default=True, env='SAVE_RAW_JSON')
    save_formatted_text: bool = Field(default=True, env='SAVE_FORMATTED_TEXT')
    save_metadata_csv: bool = Field(default=True, env='SAVE_METADATA_CSV')
    
    class Config:
        env_file = '.env'
        case_sensitive = False
    
    @validator('download_start_date', 'download_end_date')
    def validate_date_format(cls, v):
        """Validate date format (YYYY-MM-DD)."""
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('Date must be in YYYY-MM-DD format')
    
    @validator('gong_subdomain')
    def validate_subdomain(cls, v):
        """Clean and validate Gong subdomain."""
        # Remove any protocol or domain parts if user included them
        subdomain = v.lower()
        if '://' in subdomain:
            subdomain = subdomain.split('://')[-1]
        if '.gong.io' in subdomain:
            subdomain = subdomain.replace('.gong.io', '')
        if '/' in subdomain:
            subdomain = subdomain.split('/')[0]
        return subdomain
    
    @validator('output_directory')
    def validate_output_directory(cls, v):
        """Ensure output directory exists or can be created."""
        path = Path(v)
        try:
            path.mkdir(parents=True, exist_ok=True)
            return str(path)
        except Exception as e:
            raise ValueError(f'Cannot create output directory {v}: {e}')
    
    @property
    def gong_base_url(self) -> str:
        """Get the base URL for Gong API."""
        return f"https://{self.gong_subdomain}.api.gong.io"
    
    @property
    def start_date_obj(self) -> date:
        """Get start date as datetime.date object."""
        return datetime.strptime(self.download_start_date, '%Y-%m-%d').date()
    
    @property
    def end_date_obj(self) -> date:
        """Get end date as datetime.date object."""
        return datetime.strptime(self.download_end_date, '%Y-%m-%d').date()
    
    @property
    def output_path(self) -> Path:
        """Get output directory as Path object."""
        return Path(self.output_directory)
    
    def get_auth_header(self) -> str:
        """Generate Basic Auth header for Gong API."""
        import base64
        credentials = f"{self.gong_access_key}:{self.gong_access_key_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"


def load_config() -> GongConfig:
    """Load and validate configuration."""
    try:
        return GongConfig()
    except Exception as e:
        print(f"Configuration error: {e}")
        print("\nPlease ensure you have set the required environment variables:")
        print("- GONG_ACCESS_KEY")
        print("- GONG_ACCESS_KEY_SECRET") 
        print("- GONG_SUBDOMAIN")
        print("\nYou can set these in a .env file or as environment variables.")
        raise


# Example of how to use environment variables (for documentation)
REQUIRED_ENV_VARS = {
    'GONG_ACCESS_KEY': 'Your Gong API Access Key',
    'GONG_ACCESS_KEY_SECRET': 'Your Gong API Access Key Secret',
    'GONG_SUBDOMAIN': 'Your Gong subdomain (e.g., "acme" if URL is acme.gong.io)',
}

OPTIONAL_ENV_VARS = {
    'DOWNLOAD_START_DATE': 'Start date for downloads (YYYY-MM-DD, default: 2022-01-01)',
    'DOWNLOAD_END_DATE': 'End date for downloads (YYYY-MM-DD, default: 2024-12-31)',
    'OUTPUT_DIRECTORY': 'Directory to save transcripts (default: ./transcripts)',
    'MAX_CONCURRENT_DOWNLOADS': 'Max concurrent API calls (default: 3)',
    'API_RATE_LIMIT': 'API calls per second (default: 2.5)',
} 