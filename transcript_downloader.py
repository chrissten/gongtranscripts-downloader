"""
Main transcript downloader orchestrator
"""
import json
import csv
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

import aiofiles
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from config import GongConfig
from gong_client import GongAPIClient

logger = logging.getLogger(__name__)
console = Console()


class TranscriptDownloader:
    """
    Main class that orchestrates the transcript downloading process.
    
    Features:
    - Downloads call metadata and transcripts
    - Organizes files by date/speaker
    - Multiple export formats (JSON, TXT, CSV)
    - Resume capability for interrupted downloads
    - Progress tracking and logging
    """
    
    def __init__(self, config: GongConfig):
        self.config = config
        
        # Extract year from date range for folder organization
        start_year = datetime.strptime(config.download_start_date, '%Y-%m-%d').year
        end_year = datetime.strptime(config.download_end_date, '%Y-%m-%d').year
        
        # If date range spans multiple years, use the start year
        # You can modify this logic if you want different behavior
        self.year = start_year
        
        # Create year-specific output path
        self.output_path = Path(config.output_directory) / str(self.year)
        
        # Setup directories and logging
        self.setup_directories()
        self.setup_logging()
        
        # File paths
        self.metadata_file = self.output_path / "calls_metadata.csv"
        self.progress_file = self.output_path / "download_progress.json"
    
    def setup_directories(self):
        """Create necessary directories."""
        # Create main output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.output_path / "raw_json").mkdir(exist_ok=True)
        (self.output_path / "transcripts").mkdir(exist_ok=True)
        (self.output_path / "by_date").mkdir(exist_ok=True)
        (self.output_path / "by_speaker").mkdir(exist_ok=True)
        (self.output_path / "by_participant").mkdir(exist_ok=True)  # New participant organization
        (self.output_path / "logs").mkdir(exist_ok=True)
    
    def setup_logging(self):
        """Setup logging configuration."""
        log_file = self.output_path / "logs" / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        logger.info(f"Starting transcript download session")
        logger.info(f"Output directory: {self.output_path}")
        logger.info(f"Date range: {self.config.download_start_date} to {self.config.download_end_date}")
    
    async def download_all_transcripts(self, title_filter: str = None) -> Dict[str, Any]:
        """
        Main method to download all transcripts in the specified date range.
        
        Returns:
            Summary statistics of the download process
        """
        console.print(Panel.fit("🚀 Gong Transcripts Downloader", style="bold blue"))
        
        # Load previous progress if exists
        progress_data = self.load_progress()
        start_time = datetime.now()
        
        async with GongAPIClient(self.config) as client:
            # Test connection first
            if not await client.test_connection():
                raise Exception("Failed to connect to Gong API. Please check your credentials.")
            
            try:
                # Step 1: Get all calls in date range
                console.print("\n[bold yellow]Step 1:[/bold yellow] Discovering calls...")
                calls = await self.get_calls_with_resume(client, progress_data)
                # --- Title filtering logic ---
                if title_filter:
                    filter_str = title_filter.strip().lower()
                    if ' and ' in filter_str:
                        keywords = [k.strip() for k in filter_str.split(' and ') if k.strip()]
                        def match(title):
                            t = (title or '').lower()
                            return all(kw in t for kw in keywords)
                    else:
                        keywords = [k.strip() for k in filter_str.replace(',', ' ').split() if k.strip()]
                        def match(title):
                            t = (title or '').lower()
                            return any(kw in t for kw in keywords)
                    calls = [call for call in calls if match(call.get('title', ''))]
                
                if not calls:
                    console.print("[yellow]No calls found in the specified date range.[/yellow]")
                    return {"total_calls": 0, "downloaded_transcripts": 0}
                
                # Step 2: Download transcripts
                console.print(f"\n[bold yellow]Step 2:[/bold yellow] Downloading transcripts for {len(calls)} calls...")
                transcripts = await self.download_transcripts_with_resume(client, calls, progress_data)
                
                # Step 3: Process and save data
                console.print(f"\n[bold yellow]Step 3:[/bold yellow] Processing and organizing data...")
                await self.process_and_save_data(calls, transcripts)
                
                # Step 4: Generate summary
                end_time = datetime.now()
                summary = self.generate_summary(calls, transcripts, start_time, end_time)
                
                # Clean up progress file
                if self.progress_file.exists():
                    self.progress_file.unlink()
                
                console.print("\n[bold green]✅ Download completed successfully![/bold green]")
                self.display_summary(summary)
                
                return summary
                
            except Exception as e:
                logger.error(f"Download failed: {e}")
                # Save progress for resume
                await self.save_progress(progress_data)
                raise
    
    async def get_calls_with_resume(self, client: GongAPIClient, progress_data: Dict) -> List[Dict[str, Any]]:
        """Get calls list with resume capability using extensive endpoint for participant data."""
        if 'calls' in progress_data and progress_data['calls']:
            console.print(f"[blue]Resuming from previous session with {len(progress_data['calls'])} calls[/blue]")
            return progress_data['calls']
        
        # Use extensive endpoint to get participant data
        calls = await client.get_calls_list_extensive(
            self.config.download_start_date,
            self.config.download_end_date
        )
        
        # Extract call data from metaData structure
        extracted_calls = []
        for call in calls:
            if 'metaData' in call and isinstance(call['metaData'], dict):
                # Extract the call data from metaData
                call_data = call['metaData'].copy()
                
                # Add any additional fields from the extensive response
                if 'parties' in call:
                    call_data['parties'] = call['parties']
                if 'context' in call:
                    call_data['context'] = call['context']
                if 'content' in call:
                    call_data['content'] = call['content']
                if 'interaction' in call:
                    call_data['interaction'] = call['interaction']
                
                extracted_calls.append(call_data)
            else:
                # Fallback: use the call as-is if no metaData structure
                extracted_calls.append(call)
        
        progress_data['calls'] = extracted_calls
        progress_data['downloaded_call_ids'] = set()
        await self.save_progress(progress_data)
        
        return extracted_calls
    
    async def download_transcripts_with_resume(self, client: GongAPIClient, calls: List[Dict], progress_data: Dict) -> Dict[str, Dict]:
        """Download transcripts with resume capability."""
        downloaded_call_ids = set(progress_data.get('downloaded_call_ids', []))
        call_ids_to_download = []
        
        for call in calls:
            call_id = call.get('id')
            if call_id and call_id not in downloaded_call_ids:
                call_ids_to_download.append(call_id)
        
        if not call_ids_to_download:
            console.print("[blue]All transcripts already downloaded[/blue]")
            # Load existing transcripts from files
            return await self.load_existing_transcripts(downloaded_call_ids)
        
        console.print(f"Downloading {len(call_ids_to_download)} new transcripts...")
        
        # Extract workspace ID from the first call (all calls should have the same workspace)
        workspace_id = calls[0].get('workspaceId') if calls else None
        if not workspace_id:
            console.print("[red]Warning: No workspace ID found in calls[/red]")
        
        transcripts = await client.get_call_transcripts(call_ids_to_download, workspace_id)
        
        # Update progress
        if not isinstance(progress_data['downloaded_call_ids'], set):
            progress_data['downloaded_call_ids'] = set(progress_data['downloaded_call_ids'])
        progress_data['downloaded_call_ids'].update(transcripts.keys())
        await self.save_progress(progress_data)
        
        # Also load any previously downloaded transcripts
        existing_transcripts = await self.load_existing_transcripts(downloaded_call_ids)
        transcripts.update(existing_transcripts)
        
        return transcripts
    
    async def process_and_save_data(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Process and save all data with enhanced participant tracking."""
        logger.info("Processing and saving data...")
        
        # DEBUG: Log sample call structure to validate assumptions
        if calls:
            sample_call = calls[0]
            logger.info(f"Sample call ID: {sample_call.get('id')}")
            logger.info(f"Sample call keys: {list(sample_call.keys())}")
            logger.info(f"Sample call 'parties' type: {type(sample_call.get('parties'))}")
            if sample_call.get('parties'):
                logger.info(f"Sample party type: {type(sample_call['parties'][0]) if sample_call['parties'] else 'N/A'}")
            logger.info(f"Sample call 'crmObjects' type: {type(sample_call.get('crmObjects'))}")
        
        # DEBUG: Log sample transcript structure to validate assumptions
        if transcripts:
            sample_transcript_key = list(transcripts.keys())[0]
            sample_transcript = transcripts[sample_transcript_key]
            logger.info(f"Sample transcript key: {sample_transcript_key}")
            logger.info(f"Sample transcript keys: {list(sample_transcript.keys())}")
            logger.info(f"Sample transcript 'transcript' type: {type(sample_transcript.get('transcript'))}")
            if sample_transcript.get('transcript'):
                transcript_data = sample_transcript['transcript']
                logger.info(f"Sample transcript.transcript type: {type(transcript_data)}")
                if isinstance(transcript_data, dict):
                    logger.info(f"Sample transcript.transcript keys: {list(transcript_data.keys())}")
                    if transcript_data.get('transcript'):
                        logger.info(f"Sample transcript.transcript.transcript type: {type(transcript_data['transcript'])}")
                        if isinstance(transcript_data['transcript'], list) and transcript_data['transcript']:
                            logger.info(f"Sample transcript entry type: {type(transcript_data['transcript'][0])}")
        
        # Build participant profiles
        logger.info("Building participant profiles...")
        participant_profiles = self.build_participant_profiles(calls)
        
        # Save raw JSON data
        await self.save_raw_json_data(calls, transcripts)
        
        # Save formatted transcripts
        await self.save_formatted_transcripts(calls, transcripts)
        
        # Save enhanced metadata with detailed participant info
        await self.save_enhanced_metadata_csv(calls, transcripts)
        
        # Save participant profiles
        await self.save_participant_profiles(participant_profiles)
        
        # Organize by participant
        await self.organize_by_participant(calls, transcripts)
        
        logger.info("Data processing complete!")
    
    async def save_raw_json_data(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Save raw JSON data."""
        # Save individual call files
        for call in calls:
            call_id = call.get('id')
            if call_id:
                file_path = self.output_path / "raw_json" / f"call_{call_id}.json"
                call_data = {
                    'call_metadata': call,
                    'transcript': transcripts.get(call_id, {})
                }
                
                async with aiofiles.open(file_path, 'w') as f:
                    await f.write(json.dumps(call_data, indent=2))
        
        # Save consolidated file
        consolidated_data = {
            'calls': calls,
            'transcripts': transcripts,
            'download_info': {
                'date_range': f"{self.config.download_start_date} to {self.config.download_end_date}",
                'downloaded_at': datetime.now().isoformat(),
                'total_calls': len(calls),
                'total_transcripts': len(transcripts)
            }
        }
        
        consolidated_file = self.output_path / "raw_json" / "all_data.json"
        async with aiofiles.open(consolidated_file, 'w') as f:
            await f.write(json.dumps(consolidated_data, indent=2))
    
    async def save_formatted_transcripts(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Save formatted text transcripts."""
        for call in calls:
            call_id = call.get('id')
            if not call_id:  # Skip if no call ID
                continue
                
            transcript_data = transcripts.get(call_id)
            
            if not transcript_data:
                continue
            
            # Extract call metadata
            call_date = self.extract_call_date(call)
            participants = self.extract_participants(call)
            
            # Format transcript
            formatted_transcript = self.format_transcript_text(call, transcript_data)
            
            # Save to transcripts directory
            safe_filename = self.make_safe_filename(f"transcript_{call_id}_{call_date}")
            transcript_file = self.output_path / "transcripts" / f"{safe_filename}.txt"
            
            async with aiofiles.open(transcript_file, 'w', encoding='utf-8') as f:
                await f.write(formatted_transcript)
            
            # Also organize by date
            date_dir = self.output_path / "by_date" / call_date
            date_dir.mkdir(exist_ok=True)
            
            date_file = date_dir / f"{safe_filename}.txt"
            async with aiofiles.open(date_file, 'w', encoding='utf-8') as f:
                await f.write(formatted_transcript)
    
    async def save_metadata_csv(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Save metadata in CSV format."""
        metadata_rows = []
        
        for call in calls:
            call_id = call.get('id')
            if not call_id:  # Skip if no call ID
                continue
                
            transcript_data = transcripts.get(call_id, {})
            
            # Extract key metadata
            row = {
                'call_id': call_id,
                'date': self.extract_call_date(call),
                'time': self.extract_call_time(call),
                'duration_minutes': self.extract_duration(call),
                'participants': '; '.join(self.extract_participants(call)),
                'internal_participants': '; '.join(self.extract_internal_participants(call)),
                'external_participants': '; '.join(self.extract_external_participants(call)),
                'title': call.get('title', ''),
                'direction': call.get('direction', ''),
                'has_transcript': bool(transcript_data),
                'transcript_length': len(str(transcript_data)) if transcript_data else 0,
                'meeting_url': call.get('meetingUrl', ''),
                'crm_objects': '; '.join([obj.get('objectName', '') for obj in call.get('crmObjects', [])]),
            }
            
            metadata_rows.append(row)
        
        # Save as CSV
        df = pd.DataFrame(metadata_rows)
        df.to_csv(self.metadata_file, index=False)
        
        # Also save summary statistics
        summary_file = self.output_path / "summary_statistics.csv"
        summary_stats = self.calculate_summary_stats(metadata_rows)
        pd.DataFrame([summary_stats]).to_csv(summary_file, index=False)
    
    def format_transcript_text(self, call: Dict, transcript_data: Dict) -> str:
        """Format transcript data into readable text."""
        lines = []
        
        # DEBUG: Log transcript structure for debugging
        logger.debug(f"Formatting transcript for call {call.get('id')}")
        logger.debug(f"Transcript data keys: {list(transcript_data.keys())}")
        logger.debug(f"Transcript data 'transcript' type: {type(transcript_data.get('transcript'))}")
        
        # Header
        lines.append("=" * 80)
        lines.append(f"CALL TRANSCRIPT")
        lines.append("=" * 80)
        lines.append(f"Call ID: {call.get('id', 'Unknown')}")
        lines.append(f"Date: {self.extract_call_date(call)}")
        lines.append(f"Time: {self.extract_call_time(call)}")
        lines.append(f"Duration: {self.extract_duration(call)} minutes")
        lines.append(f"Title: {call.get('title', 'N/A')}")
        lines.append(f"Direction: {call.get('direction', 'N/A')}")
        
        participants = self.extract_participants(call)
        if participants:
            lines.append(f"Participants: {', '.join(participants)}")
        
        lines.append("-" * 80)
        lines.append("")
        
        # Transcript content - Fix the nested structure parsing
        # DEBUG: The actual structure is transcript_data['transcript'] is a list, not a dict
        transcript_entries = transcript_data.get('transcript', [])
        
        # DEBUG: Log transcript entries structure
        logger.debug(f"Transcript entries type: {type(transcript_entries)}")
        if isinstance(transcript_entries, list) and transcript_entries:
            logger.debug(f"First transcript entry type: {type(transcript_entries[0])}")
            if isinstance(transcript_entries[0], dict):
                logger.debug(f"First transcript entry keys: {list(transcript_entries[0].keys())}")
        
        if transcript_entries:
            for entry in transcript_entries:
                # DEBUG: Log each entry structure
                if not isinstance(entry, dict):
                    logger.warning(f"Expected transcript entry to be dict, got {type(entry)}")
                    continue
                    
                speaker = entry.get('speakerId', 'Unknown Speaker')
                topic = entry.get('topic', '')
                sentences = entry.get('sentences', [])
                
                # DEBUG: Log sentences structure
                if not isinstance(sentences, list):
                    logger.warning(f"Expected sentences to be list, got {type(sentences)}")
                    continue
                
                for sentence in sentences:
                    if not isinstance(sentence, dict):
                        logger.warning(f"Expected sentence to be dict, got {type(sentence)}")
                        continue
                        
                    text = sentence.get('text', '')
                    start_time = sentence.get('start', 0)
                    
                    # Convert milliseconds to MM:SS format
                    minutes = int(start_time // 60000)
                    seconds = int((start_time % 60000) // 1000)
                    timestamp = f"[{minutes:02d}:{seconds:02d}]"
                    
                    # Add topic if available and different from previous
                    if topic:
                        lines.append(f"{timestamp} {speaker} ({topic}): {text}")
                    else:
                        lines.append(f"{timestamp} {speaker}: {text}")
        else:
            lines.append("No transcript available for this call.")
        
        lines.append("")
        lines.append("=" * 80)
        
        return '\n'.join(lines)
    
    def extract_call_date(self, call: Dict) -> str:
        """Extract call date in YYYY-MM-DD format."""
        started = call.get('started')
        if started:
            try:
                dt = datetime.fromisoformat(started.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except:
                pass
        return 'unknown-date'
    
    def extract_call_time(self, call: Dict) -> str:
        """Extract call time in HH:MM format."""
        started = call.get('started')
        if started:
            try:
                dt = datetime.fromisoformat(started.replace('Z', '+00:00'))
                return dt.strftime('%H:%M')
            except:
                pass
        return 'unknown-time'
    
    def extract_duration(self, call: Dict) -> int:
        """Extract call duration in minutes."""
        duration = call.get('duration', 0)
        return int(duration // 60000) if duration else 0  # Convert from milliseconds
    
    def extract_participants(self, call: Dict) -> List[str]:
        """Extract all participant names."""
        participants = []
        parties = call.get('parties', [])
        
        for party in parties:
            name = party.get('name') or party.get('emailAddress', 'Unknown')
            participants.append(name)
        
        return participants
    
    def extract_internal_participants(self, call: Dict) -> List[str]:
        """Extract internal participant names."""
        participants = []
        parties = call.get('parties', [])
        
        for party in parties:
            if party.get('context') == 'Internal':
                name = party.get('name') or party.get('emailAddress', 'Unknown')
                participants.append(name)
        
        return participants
    
    def extract_external_participants(self, call: Dict) -> List[str]:
        """Extract external participant names."""
        participants = []
        parties = call.get('parties', [])
        
        for party in parties:
            if party.get('context') == 'External':
                name = party.get('name') or party.get('emailAddress', 'Unknown')
                participants.append(name)
        
        return participants
    
    def extract_detailed_participants(self, call: Dict) -> List[Dict]:
        """Extract detailed participant information for enhanced tracking."""
        detailed_participants = []
        parties = call.get('parties', [])
        if not isinstance(parties, list):
            logger.warning(f"Expected 'parties' to be a list but got {type(parties)} in call {call.get('id')}")
            return detailed_participants
        for party in parties:
            if not isinstance(party, dict):
                logger.warning(f"Expected party to be a dict but got {type(party)} in call {call.get('id')}")
                continue
            participant = {
                'name': party.get('name', ''),
                'email': party.get('emailAddress', ''),
                'context': party.get('context', ''),  # Internal/External
                'role': party.get('role', ''),
                'company': party.get('company', ''),
                'title': party.get('title', ''),
                'speaker_id': party.get('speakerId', ''),
                'is_host': party.get('isHost', False),
                'is_organizer': party.get('isOrganizer', False),
            }
            detailed_participants.append(participant)
        return detailed_participants
    
    def extract_participant_names_with_context(self, call: Dict) -> Dict[str, List[str]]:
        """Extract participant names organized by context for easy filtering."""
        internal_names = []
        external_names = []
        all_names = []
        parties = call.get('parties', [])
        if not isinstance(parties, list):
            logger.warning(f"Expected 'parties' to be a list but got {type(parties)} in call {call.get('id')}")
            return {'all': [], 'internal': [], 'external': []}
        for party in parties:
            if not isinstance(party, dict):
                logger.warning(f"Expected party to be a dict but got {type(party)} in call {call.get('id')}")
                continue
            name = party.get('name') or party.get('emailAddress', 'Unknown')
            context = party.get('context', '')
            all_names.append(name)
            if context == 'Internal':
                internal_names.append(name)
            elif context == 'External':
                external_names.append(name)
        return {
            'all': all_names,
            'internal': internal_names,
            'external': external_names
        }
    
    def create_participant_key(self, participant: Dict) -> str:
        """Create a unique key for a participant based on email or name."""
        email = participant.get('email', '').strip()
        name = participant.get('name', '').strip()
        
        if email:
            return email.lower()
        elif name:
            return name.lower()
        else:
            return 'unknown'
    
    def build_participant_profiles(self, calls: List[Dict]) -> Dict[str, Dict]:
        """Build comprehensive participant profiles from all calls."""
        participant_profiles = {}
        for call in calls:
            if not isinstance(call, dict):
                logger.warning(f"Expected call to be a dict but got {type(call)}")
                continue
            call_id = call.get('id')
            if not call_id:
                continue
            detailed_participants = self.extract_detailed_participants(call)
            call_date = self.extract_call_date(call)
            duration = self.extract_duration(call)
            for participant in detailed_participants:
                if not isinstance(participant, dict):
                    logger.warning(f"Expected participant to be a dict but got {type(participant)} in call {call_id}")
                    continue
                participant_key = self.create_participant_key(participant)
                if participant_key not in participant_profiles:
                    participant_profiles[participant_key] = {
                        'name': participant.get('name', ''),
                        'email': participant.get('email', ''),
                        'context': participant.get('context', ''),
                        'role': participant.get('role', ''),
                        'company': participant.get('company', ''),
                        'title': participant.get('title', ''),
                        'speaker_id': participant.get('speaker_id', ''),
                        'total_calls': 0,
                        'total_duration_minutes': 0,
                        'call_dates': [],
                        'call_ids': [],
                        'is_host_count': 0,
                        'is_organizer_count': 0,
                        'first_seen': call_date,
                        'last_seen': call_date,
                    }
                profile = participant_profiles[participant_key]
                profile['total_calls'] += 1
                profile['total_duration_minutes'] += duration
                profile['call_dates'].append(call_date)
                profile['call_ids'].append(call_id)
                if participant.get('is_host'):
                    profile['is_host_count'] += 1
                if participant.get('is_organizer'):
                    profile['is_organizer_count'] += 1
                if call_date < profile['first_seen']:
                    profile['first_seen'] = call_date
                if call_date > profile['last_seen']:
                    profile['last_seen'] = call_date
        return participant_profiles
    
    def make_safe_filename(self, filename: str) -> str:
        """Make filename safe for filesystem."""
        import re
        # Remove or replace unsafe characters
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Limit length
        if len(safe_name) > 200:
            safe_name = safe_name[:200]
        return safe_name
    
    def calculate_summary_stats(self, metadata_rows: List[Dict]) -> Dict:
        """Calculate summary statistics."""
        if not metadata_rows:
            return {}
        
        df = pd.DataFrame(metadata_rows)
        
        return {
            'total_calls': len(metadata_rows),
            'calls_with_transcripts': sum(1 for row in metadata_rows if row['has_transcript']),
            'date_range_start': df['date'].min() if 'date' in df else '',
            'date_range_end': df['date'].max() if 'date' in df else '',
            'total_duration_minutes': df['duration_minutes'].sum() if 'duration_minutes' in df else 0,
            'average_duration_minutes': df['duration_minutes'].mean() if 'duration_minutes' in df else 0,
            'unique_internal_participants': len(set('; '.join(df['internal_participants']).split('; '))),
            'unique_external_participants': len(set('; '.join(df['external_participants']).split('; '))),
        }
    
    def generate_summary(self, calls: List[Dict], transcripts: Dict, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Generate download summary."""
        duration = end_time - start_time
        
        return {
            'total_calls': len(calls),
            'downloaded_transcripts': len(transcripts),
            'success_rate': len(transcripts) / len(calls) if calls else 0,
            'download_duration': str(duration),
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'output_directory': str(self.output_path),
        }
    
    def display_summary(self, summary: Dict[str, Any]):
        """Display a nice summary table."""
        table = Table(title="Download Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Calls Found", str(summary['total_calls']))
        table.add_row("Transcripts Downloaded", str(summary['downloaded_transcripts']))
        table.add_row("Success Rate", f"{summary['success_rate']:.1%}")
        table.add_row("Duration", summary['download_duration'])
        table.add_row("Output Directory", summary['output_directory'])
        
        console.print(table)
    
    async def load_existing_transcripts(self, call_ids: set) -> Dict[str, Dict]:
        """Load existing transcripts from files."""
        transcripts = {}
        
        for call_id in call_ids:
            file_path = self.output_path / "raw_json" / f"call_{call_id}.json"
            if file_path.exists():
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        content = await f.read()
                        data = json.loads(content)
                        transcript = data.get('transcript', {})
                        if transcript:
                            transcripts[call_id] = transcript
                except Exception as e:
                    logger.warning(f"Could not load existing transcript for call {call_id}: {e}")
        
        return transcripts
    
    async def save_progress(self, progress_data: Dict):
        """Save progress to file for resume capability."""
        # Convert sets to lists for JSON serialization
        if 'downloaded_call_ids' in progress_data:
            progress_data['downloaded_call_ids'] = list(progress_data['downloaded_call_ids'])
        
        async with aiofiles.open(self.progress_file, 'w') as f:
            await f.write(json.dumps(progress_data, indent=2))
    
    def load_progress(self) -> Dict:
        """Load progress from file."""
        if not self.progress_file.exists():
            return {}
        
        try:
            with open(self.progress_file, 'r') as f:
                progress_data = json.load(f)
                
            # Convert lists back to sets
            if 'downloaded_call_ids' in progress_data:
                progress_data['downloaded_call_ids'] = set(progress_data['downloaded_call_ids'])
            
            return progress_data
        except Exception as e:
            logger.warning(f"Could not load progress file: {e}")
            return {}
    
    async def save_participant_profiles(self, participant_profiles: Dict[str, Dict]):
        """Save participant profiles to CSV for easy filtering and analysis."""
        if not participant_profiles:
            return
        
        # Convert to list for CSV export
        profile_rows = []
        for participant_key, profile in participant_profiles.items():
            row = {
                'participant_key': participant_key,
                'name': profile['name'],
                'email': profile['email'],
                'context': profile['context'],
                'role': profile['role'],
                'company': profile['company'],
                'title': profile['title'],
                'speaker_id': profile['speaker_id'],
                'total_calls': profile['total_calls'],
                'total_duration_minutes': profile['total_duration_minutes'],
                'average_duration_minutes': profile['total_duration_minutes'] / profile['total_calls'] if profile['total_calls'] > 0 else 0,
                'is_host_count': profile['is_host_count'],
                'is_organizer_count': profile['is_organizer_count'],
                'first_seen': profile['first_seen'],
                'last_seen': profile['last_seen'],
                'call_ids': '; '.join(profile['call_ids']),
                'call_dates': '; '.join(sorted(set(profile['call_dates']))),
            }
            profile_rows.append(row)
        
        # Save participant profiles CSV
        participants_file = self.output_path / "participants.csv"
        df = pd.DataFrame(profile_rows)
        df.to_csv(participants_file, index=False)
        
        # Save participant summary statistics
        summary_file = self.output_path / "participant_summary.csv"
        summary_stats = self.calculate_participant_summary_stats(profile_rows)
        pd.DataFrame([summary_stats]).to_csv(summary_file, index=False)
        
        logger.info(f"Saved {len(profile_rows)} participant profiles to {participants_file}")
    
    def calculate_participant_summary_stats(self, profile_rows: List[Dict]) -> Dict:
        """Calculate summary statistics for participants."""
        if not profile_rows:
            return {}
        
        df = pd.DataFrame(profile_rows)
        
        return {
            'total_unique_participants': len(profile_rows),
            'internal_participants': len(df[df['context'] == 'Internal']),
            'external_participants': len(df[df['context'] == 'External']),
            'participants_with_emails': len(df[df['email'] != '']),
            'participants_with_companies': len(df[df['company'] != '']),
            'most_active_participant': df.loc[df['total_calls'].idxmax(), 'name'] if len(df) > 0 else '',
            'highest_total_duration': df['total_duration_minutes'].max() if len(df) > 0 else 0,
            'average_calls_per_participant': df['total_calls'].mean() if len(df) > 0 else 0,
            'average_duration_per_participant': df['total_duration_minutes'].mean() if len(df) > 0 else 0,
        }
    
    async def save_enhanced_metadata_csv(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Save enhanced metadata with detailed participant information."""
        metadata_rows = []
        
        for call in calls:
            call_id = call.get('id')
            if not call_id:
                continue
                
            transcript_data = transcripts.get(call_id, {})
            detailed_participants = self.extract_detailed_participants(call)
            participant_contexts = self.extract_participant_names_with_context(call)
            
            # Defensive: crmObjects should be a list of dicts
            crm_objects = call.get('crmObjects', [])
            crm_object_names = []
            if isinstance(crm_objects, list):
                for obj in crm_objects:
                    if isinstance(obj, dict):
                        crm_object_names.append(obj.get('objectName', ''))
                    else:
                        logger.warning(f"Unexpected crmObject type: {type(obj)} in call {call_id}")
            else:
                logger.warning(f"crmObjects is not a list in call {call_id}: {type(crm_objects)}")
            
            # Extract key metadata with enhanced participant info
            row = {
                'call_id': call_id,
                'date': self.extract_call_date(call),
                'time': self.extract_call_time(call),
                'duration_minutes': self.extract_duration(call),
                'title': call.get('title', ''),
                'direction': call.get('direction', ''),
                'has_transcript': bool(transcript_data),
                'transcript_length': len(str(transcript_data)) if transcript_data else 0,
                'meeting_url': call.get('meetingUrl', ''),
                'crm_objects': '; '.join(crm_object_names),
                # Enhanced participant information
                'all_participants': '; '.join(participant_contexts['all']),
                'internal_participants': '; '.join(participant_contexts['internal']),
                'external_participants': '; '.join(participant_contexts['external']),
                'participant_count': len(detailed_participants),
                'internal_count': len(participant_contexts['internal']),
                'external_count': len(participant_contexts['external']),
                # Individual participant details (for easy filtering)
                'participant_1_name': detailed_participants[0].get('name', '') if detailed_participants else '',
                'participant_1_email': detailed_participants[0].get('email', '') if detailed_participants else '',
                'participant_1_context': detailed_participants[0].get('context', '') if detailed_participants else '',
                'participant_1_role': detailed_participants[0].get('role', '') if detailed_participants else '',
                'participant_1_company': detailed_participants[0].get('company', '') if detailed_participants else '',
                'participant_2_name': detailed_participants[1].get('name', '') if len(detailed_participants) > 1 else '',
                'participant_2_email': detailed_participants[1].get('email', '') if len(detailed_participants) > 1 else '',
                'participant_2_context': detailed_participants[1].get('context', '') if len(detailed_participants) > 1 else '',
                'participant_2_role': detailed_participants[1].get('role', '') if len(detailed_participants) > 1 else '',
                'participant_2_company': detailed_participants[1].get('company', '') if len(detailed_participants) > 1 else '',
                'participant_3_name': detailed_participants[2].get('name', '') if len(detailed_participants) > 2 else '',
                'participant_3_email': detailed_participants[2].get('email', '') if len(detailed_participants) > 2 else '',
                'participant_3_context': detailed_participants[2].get('context', '') if len(detailed_participants) > 2 else '',
                'participant_3_role': detailed_participants[2].get('role', '') if len(detailed_participants) > 2 else '',
                'participant_3_company': detailed_participants[2].get('company', '') if len(detailed_participants) > 2 else '',
                'participant_4_name': detailed_participants[3].get('name', '') if len(detailed_participants) > 3 else '',
                'participant_4_email': detailed_participants[3].get('email', '') if len(detailed_participants) > 3 else '',
                'participant_4_context': detailed_participants[3].get('context', '') if len(detailed_participants) > 3 else '',
                'participant_4_role': detailed_participants[3].get('role', '') if len(detailed_participants) > 3 else '',
                'participant_4_company': detailed_participants[3].get('company', '') if len(detailed_participants) > 3 else '',
                'participant_5_name': detailed_participants[4].get('name', '') if len(detailed_participants) > 4 else '',
                'participant_5_email': detailed_participants[4].get('email', '') if len(detailed_participants) > 4 else '',
                'participant_5_context': detailed_participants[4].get('context', '') if len(detailed_participants) > 4 else '',
                'participant_5_role': detailed_participants[4].get('role', '') if len(detailed_participants) > 4 else '',
                'participant_5_company': detailed_participants[4].get('company', '') if len(detailed_participants) > 4 else '',
            }
            
            metadata_rows.append(row)
        
        # Save enhanced metadata CSV
        df = pd.DataFrame(metadata_rows)
        df.to_csv(self.metadata_file, index=False)
        
        # Also save summary statistics
        summary_file = self.output_path / "summary_statistics.csv"
        summary_stats = self.calculate_summary_stats(metadata_rows)
        pd.DataFrame([summary_stats]).to_csv(summary_file, index=False)
        
        logger.info(f"Saved enhanced metadata for {len(metadata_rows)} calls to {self.metadata_file}")
    
    async def organize_by_participant(self, calls: List[Dict], transcripts: Dict[str, Dict]):
        """Organize transcripts by individual participants for easy access."""
        participant_calls = {}
        
        for call in calls:
            call_id = call.get('id')
            if not call_id:
                continue
                
            detailed_participants = self.extract_detailed_participants(call)
            transcript_data = transcripts.get(call_id)
            
            if not transcript_data:
                continue
            
            # Format transcript
            formatted_transcript = self.format_transcript_text(call, transcript_data)
            call_date = self.extract_call_date(call)
            safe_filename = self.make_safe_filename(f"transcript_{call_id}_{call_date}")
            
            # Organize by each participant
            for participant in detailed_participants:
                participant_key = self.create_participant_key(participant)
                participant_name = participant.get('name', 'Unknown')
                
                if participant_key not in participant_calls:
                    participant_calls[participant_key] = {
                        'name': participant_name,
                        'email': participant.get('email', ''),
                        'context': participant.get('context', ''),
                        'calls': []
                    }
                
                participant_calls[participant_key]['calls'].append({
                    'call_id': call_id,
                    'date': call_date,
                    'filename': safe_filename,
                    'transcript': formatted_transcript
                })
        
        # Save organized transcripts
        for participant_key, participant_data in participant_calls.items():
            participant_name = participant_data['name']
            safe_participant_name = self.make_safe_filename(participant_name)
            
            # Create participant directory
            participant_dir = self.output_path / "by_participant" / safe_participant_name
            participant_dir.mkdir(parents=True, exist_ok=True)
            
            # Save each call transcript
            for call_data in participant_data['calls']:
                transcript_file = participant_dir / f"{call_data['filename']}.txt"
                async with aiofiles.open(transcript_file, 'w', encoding='utf-8') as f:
                    await f.write(call_data['transcript'])
            
            # Save participant summary
            summary_file = participant_dir / "participant_summary.txt"
            summary_content = self.create_participant_summary(participant_data)
            async with aiofiles.open(summary_file, 'w', encoding='utf-8') as f:
                await f.write(summary_content)
        
        logger.info(f"Organized transcripts by {len(participant_calls)} participants")
    
    def create_participant_summary(self, participant_data: Dict) -> str:
        """Create a summary file for a participant."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"PARTICIPANT SUMMARY")
        lines.append("=" * 80)
        lines.append(f"Name: {participant_data['name']}")
        lines.append(f"Email: {participant_data['email']}")
        lines.append(f"Context: {participant_data['context']}")
        lines.append(f"Total Calls: {len(participant_data['calls'])}")
        lines.append("")
        lines.append("Call History:")
        lines.append("-" * 40)
        
        for call in participant_data['calls']:
            lines.append(f"  {call['date']} - Call ID: {call['call_id']}")
        
        lines.append("")
        lines.append("=" * 80)
        
        return '\n'.join(lines) 