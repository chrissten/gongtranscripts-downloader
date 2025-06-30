"""
Gong API Client for downloading call transcripts and metadata
"""
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Iterator, Tuple
from pathlib import Path
import logging

import aiohttp
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from rich.console import Console
from rich.progress import Progress, TimeElapsedColumn, BarColumn, TextColumn

from config import GongConfig

logger = logging.getLogger(__name__)
console = Console()


class GongAPIError(Exception):
    """Custom exception for Gong API errors."""
    pass


class GongAPIClient:
    """
    Asynchronous client for interacting with Gong API to download transcripts.
    
    Handles:
    - Authentication
    - Rate limiting
    - Pagination
    - Error handling and retries
    - Progress tracking
    """
    
    def __init__(self, config: GongConfig):
        self.config = config
        self.base_url = config.gong_base_url
        self.headers = {
            'Authorization': config.get_auth_header(),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = asyncio.Semaphore(1)  # Ensure sequential API calls for rate limiting
        self.last_request_time = 0.0
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.api_timeout),
            headers=self.headers
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self):
        """Enforce API rate limiting."""
        async with self.rate_limiter:
            now = time.time()
            time_since_last = now - self.last_request_time
            min_interval = 1.0 / self.config.api_rate_limit
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)
            
            self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an HTTP request with rate limiting and error handling."""
        await self._rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with self.session.request(method, url, **kwargs) as response:
                if response.status == 429:  # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                
                if response.status >= 400:
                    error_text = await response.text()
                    raise GongAPIError(f"API error {response.status}: {error_text}")
                
                return await response.json()
                
        except aiohttp.ClientError as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    async def get_calls_list(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get all calls within the specified date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of call metadata dictionaries
        """
        all_calls = []
        cursor = None
        page = 1
        
        # Convert dates to ISO format for API
        # fromDateTime: start of the day (inclusive)
        # toDateTime: start of the next day (exclusive) - API returns calls up to but excluding this time
        start_datetime = f"{start_date}T00:00:00Z"
        
        # Calculate the next day for toDateTime
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        next_day = end_date_obj + timedelta(days=1)
        end_datetime = f"{next_day.strftime('%Y-%m-%d')}T00:00:00Z"
        
        with Progress(
            TextColumn("[bold blue]Fetching calls list..."),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            
            # We don't know total pages initially, so start with indeterminate progress
            task = progress.add_task("Discovering calls...", total=None)
            
            while True:
                # Prepare query parameters for GET /v2/calls
                params = {
                    "fromDateTime": start_datetime,
                    "toDateTime": end_datetime
                }
                
                if cursor:
                    params["cursor"] = cursor
                
                try:
                    response = await self._make_request('GET', '/v2/calls', params=params)
                    
                    calls = response.get('calls', [])
                    
                    # DEBUG: Log sample call structure to validate API response
                    if page == 1 and calls:
                        sample_call = calls[0]
                        logger.info(f"DEBUG: Sample call ID: {sample_call.get('id')}")
                        logger.info(f"DEBUG: Sample call keys: {list(sample_call.keys())}")
                        logger.info(f"DEBUG: Sample call has 'parties' field: {'parties' in sample_call}")
                        logger.info(f"DEBUG: Sample call has 'metaData' field: {'metaData' in sample_call}")
                        if 'parties' in sample_call:
                            logger.info(f"DEBUG: Sample call parties type: {type(sample_call['parties'])}")
                            logger.info(f"DEBUG: Sample call parties count: {len(sample_call['parties']) if isinstance(sample_call['parties'], list) else 'N/A'}")
                        if 'metaData' in sample_call:
                            logger.info(f"DEBUG: Sample call metaData type: {type(sample_call['metaData'])}")
                            logger.info(f"DEBUG: Sample call metaData keys: {list(sample_call['metaData'].keys()) if isinstance(sample_call['metaData'], dict) else 'N/A'}")
                    
                    all_calls.extend(calls)
                    
                    # Update progress
                    records_info = response.get('records', {})
                    total_records = records_info.get('totalRecords', 0)
                    current_count = len(all_calls)
                    
                    if total_records > 0:
                        progress.update(task, total=total_records, completed=current_count,
                                      description=f"Fetching calls (page {page})")
                    
                    # Check if there are more pages
                    cursor = records_info.get('cursor')
                    if not cursor:
                        break
                        
                    page += 1
                    
                except Exception as e:
                    logger.error(f"Error fetching calls page {page}: {e}")
                    raise
        
        console.print(f"[green]✓[/green] Found {len(all_calls)} calls between {start_date} and {end_date}")
        return all_calls
    
    async def get_calls_list_extensive(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get all calls with extensive data (including participants) within the specified date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of call metadata dictionaries with participant information
        """
        all_calls = []
        cursor = None
        page = 1
        
        # Convert dates to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        
        # Calculate the next day for toDateTime
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        next_day = end_date_obj + timedelta(days=1)
        end_datetime = f"{next_day.strftime('%Y-%m-%d')}T00:00:00Z"
        
        with Progress(
            TextColumn("[bold blue]Fetching calls with extensive data..."),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Discovering calls with participants...", total=None)
            
            while True:
                # Prepare request body for POST /v2/calls/extensive
                body = {
                    "filter": {
                        "fromDateTime": start_datetime,
                        "toDateTime": end_datetime
                    },
                    "contentSelector": {
                        "exposedFields": {
                            "parties": True,  # Explicitly request participant data
                            "content": {
                                "structure": False,
                                "topics": False,
                                "trackers": False,
                                "trackerOccurrences": False,
                                "pointsOfInterest": False,
                                "brief": True,
                                "outline": True,
                                "highlights": True,
                                "callOutcome": True,
                                "keyPoints": True
                            },
                            "interaction": {
                                "speakers": True,
                                "video": True,
                                "personInteractionStats": True,
                                "questions": True
                            },
                            "collaboration": {
                                "publicComments": True
                            },
                            "media": True
                        }
                    }
                }
                
                if cursor:
                    body["cursor"] = cursor
                
                try:
                    response = await self._make_request('POST', '/v2/calls/extensive', json=body)
                    
                    calls = response.get('calls', [])
                    
                    # DEBUG: Log sample call structure from extensive endpoint
                    if page == 1 and calls:
                        sample_call = calls[0]
                        logger.info(f"DEBUG EXTENSIVE: Sample call ID: {sample_call.get('metaData', {}).get('id')}")
                        logger.info(f"DEBUG EXTENSIVE: Sample call keys: {list(sample_call.keys())}")
                        logger.info(f"DEBUG EXTENSIVE: Sample call has 'parties' field: {'parties' in sample_call}")
                        logger.info(f"DEBUG EXTENSIVE: Sample call has 'metaData' field: {'metaData' in sample_call}")
                        if 'parties' in sample_call:
                            logger.info(f"DEBUG EXTENSIVE: Sample call parties type: {type(sample_call['parties'])}")
                            logger.info(f"DEBUG EXTENSIVE: Sample call parties count: {len(sample_call['parties']) if isinstance(sample_call['parties'], list) else 'N/A'}")
                            if isinstance(sample_call['parties'], list) and sample_call['parties']:
                                sample_party = sample_call['parties'][0]
                                logger.info(f"DEBUG EXTENSIVE: Sample party keys: {list(sample_party.keys())}")
                                logger.info(f"DEBUG EXTENSIVE: Sample party name: {sample_party.get('name', 'N/A')}")
                                logger.info(f"DEBUG EXTENSIVE: Sample party email: {sample_party.get('emailAddress', 'N/A')}")
                        if 'metaData' in sample_call:
                            logger.info(f"DEBUG EXTENSIVE: Sample call metaData type: {type(sample_call['metaData'])}")
                            logger.info(f"DEBUG EXTENSIVE: Sample call metaData keys: {list(sample_call['metaData'].keys()) if isinstance(sample_call['metaData'], dict) else 'N/A'}")
                    
                    all_calls.extend(calls)
                    
                    # Update progress
                    records_info = response.get('records', {})
                    total_records = records_info.get('totalRecords', 0)
                    current_count = len(all_calls)
                    
                    if total_records > 0:
                        progress.update(task, total=total_records, completed=current_count,
                                      description=f"Fetching calls with participants (page {page})")
                    
                    # Check if there are more pages
                    cursor = records_info.get('cursor')
                    if not cursor:
                        break
                        
                    page += 1
                    
                except Exception as e:
                    logger.error(f"Error fetching calls page {page}: {e}")
                    raise
        
        console.print(f"[green]✓[/green] Found {len(all_calls)} calls with participant data between {start_date} and {end_date}")
        return all_calls
    
    async def get_call_transcripts(self, call_ids: List[str], workspace_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get transcripts for multiple calls.
        
        Args:
            call_ids: List of call IDs to get transcripts for
            workspace_id: Workspace ID (required by API)
            
        Returns:
            Dictionary mapping call_id to transcript data
        """
        # Gong API allows up to 100 call IDs per request
        batch_size = 100
        all_transcripts = {}
        
        with Progress(
            TextColumn("[bold blue]Downloading transcripts..."),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Fetching transcripts...", total=len(call_ids))
            
            for i in range(0, len(call_ids), batch_size):
                batch_ids = call_ids[i:i + batch_size]
                
                # Prepare request body with proper filter structure
                # Calculate the next day for toDateTime (API returns up to but excluding this time)
                end_date_obj = datetime.strptime(self.config.download_end_date, '%Y-%m-%d')
                next_day = end_date_obj + timedelta(days=1)
                end_datetime = f"{next_day.strftime('%Y-%m-%d')}T00:00:00Z"
                
                body = {
                    "filter": {
                        "fromDateTime": f"{self.config.download_start_date}T00:00:00Z",
                        "toDateTime": end_datetime,
                        "callIds": batch_ids
                    }
                }
                
                try:
                    response = await self._make_request('POST', '/v2/calls/transcript', json=body)
                    
                    # Process transcripts from response
                    call_transcripts = response.get('callTranscripts', [])
                    for transcript_data in call_transcripts:
                        call_id = transcript_data.get('callId')
                        if call_id:
                            all_transcripts[call_id] = transcript_data
                    
                    progress.update(task, advance=len(batch_ids))
                    
                except Exception as e:
                    logger.error(f"Error fetching transcripts for batch starting at {i}: {e}")
                    # Continue with next batch rather than failing completely
                    progress.update(task, advance=len(batch_ids))
                    continue
        
        console.print(f"[green]✓[/green] Downloaded {len(all_transcripts)} transcripts")
        return all_transcripts
    
    async def test_connection(self) -> bool:
        """Test API connection and credentials."""
        try:
            url = f"{self.base_url}/v2/calls"
            
            # Test with the user's configured date range
            start_datetime = f"{self.config.download_start_date}T00:00:00Z"
            
            # Calculate the next day for toDateTime
            end_date_obj = datetime.strptime(self.config.download_end_date, '%Y-%m-%d')
            next_day = end_date_obj + timedelta(days=1)
            end_datetime = f"{next_day.strftime('%Y-%m-%d')}T00:00:00Z"
            
            params = {
                "fromDateTime": start_datetime,
                "toDateTime": end_datetime
            }
            
            response = requests.get(
                url, 
                params=params, 
                headers=self.headers, 
                timeout=self.config.api_timeout
            )
            
            if response.status_code == 200:
                console.print("[green]✓[/green] API connection successful")
                return True
            else:
                console.print(f"[red]✗[/red] API connection failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            console.print(f"[red]✗[/red] API connection failed: {e}")
            return False


class GongSyncClient:
    """
    Synchronous version of GongAPIClient for simple operations.
    Useful for testing and simple scripts.
    """
    
    def __init__(self, config: GongConfig):
        self.config = config
        self.base_url = config.gong_base_url
        self.headers = {
            'Authorization': config.get_auth_header(),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def test_connection(self) -> bool:
        """Test API connection and credentials."""
        try:
            url = f"{self.base_url}/v2/calls"
            
            # Test with the user's configured date range
            start_datetime = f"{self.config.download_start_date}T00:00:00Z"
            
            # Calculate the next day for toDateTime
            end_date_obj = datetime.strptime(self.config.download_end_date, '%Y-%m-%d')
            next_day = end_date_obj + timedelta(days=1)
            end_datetime = f"{next_day.strftime('%Y-%m-%d')}T00:00:00Z"
            
            params = {
                "fromDateTime": start_datetime,
                "toDateTime": end_datetime
            }
            
            response = requests.get(
                url, 
                params=params, 
                headers=self.headers, 
                timeout=self.config.api_timeout
            )
            
            if response.status_code == 200:
                console.print("[green]✓[/green] API connection successful")
                return True
            else:
                console.print(f"[red]✗[/red] API connection failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            console.print(f"[red]✗[/red] API connection failed: {e}")
            return False
    
    def get_user_info(self) -> Optional[Dict[str, Any]]:
        """Get basic user/workspace information."""
        try:
            url = f"{self.base_url}/v2/users"
            response = requests.get(url, headers=self.headers, timeout=self.config.api_timeout)
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.error(f"Failed to get user info: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            return None 