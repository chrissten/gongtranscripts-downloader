#!/usr/bin/env python3
"""
Participant Filter Utility

This script demonstrates how to filter and analyze calls by specific participants
using the enhanced participant data collected by the transcript downloader.

Usage:
    python participant_filter.py --participant "Greg" --year 2025
    python participant_filter.py --email "greg@company.com" --year 2025
    python participant_filter.py --list-participants --year 2025
"""

import argparse
import pandas as pd
from pathlib import Path
import sys
from typing import List, Dict, Optional


class ParticipantFilter:
    def __init__(self, year: int, base_output_dir: str = "transcripts"):
        self.year = year
        self.base_output_dir = Path(base_output_dir)
        self.year_dir = self.base_output_dir / str(year)
        
        # File paths
        self.metadata_file = self.year_dir / "calls_metadata.csv"
        self.participants_file = self.year_dir / "participants.csv"
        
        if not self.year_dir.exists():
            raise FileNotFoundError(f"Year directory {self.year_dir} not found. Run the downloader first.")
    
    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load metadata and participant data."""
        if not self.metadata_file.exists():
            raise FileNotFoundError(f"Metadata file {self.metadata_file} not found.")
        
        if not self.participants_file.exists():
            raise FileNotFoundError(f"Participants file {self.participants_file} not found.")
        
        metadata_df = pd.read_csv(self.metadata_file)
        participants_df = pd.read_csv(self.participants_file)
        
        return metadata_df, participants_df
    
    def list_participants(self, context: Optional[str] = None) -> None:
        """List all participants with their statistics."""
        metadata_df, participants_df = self.load_data()
        
        if context:
            participants_df = participants_df[participants_df['context'] == context]
        
        print(f"\n{'='*80}")
        print(f"PARTICIPANTS IN {self.year}")
        print(f"{'='*80}")
        
        if context:
            print(f"Filtered by context: {context}")
        
        print(f"\nTotal participants: {len(participants_df)}")
        print(f"{'='*80}")
        
        # Sort by total calls (most active first)
        participants_df = participants_df.sort_values('total_calls', ascending=False)
        
        for _, participant in participants_df.iterrows():
            print(f"\nName: {participant['name']}")
            print(f"Email: {participant['email']}")
            print(f"Context: {participant['context']}")
            print(f"Company: {participant['company']}")
            print(f"Total Calls: {participant['total_calls']}")
            print(f"Total Duration: {participant['total_duration_minutes']:.0f} minutes")
            print(f"Average Duration: {participant['average_duration_minutes']:.1f} minutes")
            print(f"Date Range: {participant['first_seen']} to {participant['last_seen']}")
            print("-" * 40)
    
    def filter_by_participant(self, participant_name: str, participant_email: Optional[str] = None) -> pd.DataFrame:
        """Filter calls by participant name or email."""
        metadata_df, participants_df = self.load_data()
        
        # Find participant
        if participant_email:
            participant_filter = participants_df['email'].str.contains(participant_email, case=False, na=False)
        else:
            participant_filter = participants_df['name'].str.contains(participant_name, case=False, na=False)
        
        matching_participants = participants_df[participant_filter]
        
        if matching_participants.empty:
            print(f"No participants found matching '{participant_name or participant_email}'")
            return pd.DataFrame()
        
        print(f"\nFound {len(matching_participants)} matching participants:")
        for _, participant in matching_participants.iterrows():
            print(f"  - {participant['name']} ({participant['email']}) - {participant['total_calls']} calls")
        
        # Get call IDs for all matching participants
        all_call_ids = set()
        for _, participant in matching_participants.iterrows():
            call_ids = participant['call_ids'].split('; ')
            all_call_ids.update(call_ids)
        
        # Filter metadata by call IDs
        filtered_calls = metadata_df[metadata_df['call_id'].isin(all_call_ids)]
        
        return filtered_calls
    
    def analyze_participant_calls(self, participant_name: str, participant_email: Optional[str] = None) -> None:
        """Analyze calls for a specific participant."""
        filtered_calls = self.filter_by_participant(participant_name, participant_email)
        
        if filtered_calls.empty:
            return
        
        print(f"\n{'='*80}")
        print(f"CALL ANALYSIS FOR: {participant_name or participant_email}")
        print(f"{'='*80}")
        
        print(f"\nTotal calls: {len(filtered_calls)}")
        print(f"Calls with transcripts: {filtered_calls['has_transcript'].sum()}")
        print(f"Total duration: {filtered_calls['duration_minutes'].sum():.0f} minutes")
        print(f"Average duration: {filtered_calls['duration_minutes'].mean():.1f} minutes")
        
        # Date range
        print(f"Date range: {filtered_calls['date'].min()} to {filtered_calls['date'].max()}")
        
        # Call types
        if 'direction' in filtered_calls.columns:
            direction_counts = filtered_calls['direction'].value_counts()
            print(f"\nCall directions:")
            for direction, count in direction_counts.items():
                print(f"  {direction}: {count}")
        
        # Recent calls
        print(f"\nRecent calls:")
        recent_calls = filtered_calls.sort_values('date', ascending=False).head(5)
        for _, call in recent_calls.iterrows():
            print(f"  {call['date']} - {call['title']} ({call['duration_minutes']} min)")
        
        # Save filtered calls to CSV
        output_file = self.year_dir / f"calls_{participant_name or 'filtered'}.csv"
        filtered_calls.to_csv(output_file, index=False)
        print(f"\nFiltered calls saved to: {output_file}")
        
        # Show participant directory if it exists
        participant_dir = self.year_dir / "by_participant" / (participant_name or "unknown")
        if participant_dir.exists():
            print(f"\nParticipant transcripts available in: {participant_dir}")
            transcript_files = list(participant_dir.glob("*.txt"))
            print(f"  {len(transcript_files)} transcript files found")
    
    def search_participants(self, search_term: str) -> None:
        """Search for participants by name, email, or company."""
        metadata_df, participants_df = self.load_data()
        
        # Search in name, email, and company fields
        name_match = participants_df['name'].str.contains(search_term, case=False, na=False)
        email_match = participants_df['email'].str.contains(search_term, case=False, na=False)
        company_match = participants_df['company'].str.contains(search_term, case=False, na=False)
        
        matching_participants = participants_df[name_match | email_match | company_match]
        
        if matching_participants.empty:
            print(f"No participants found matching '{search_term}'")
            return
        
        print(f"\nFound {len(matching_participants)} participants matching '{search_term}':")
        print(f"{'='*80}")
        
        for _, participant in matching_participants.iterrows():
            print(f"\nName: {participant['name']}")
            print(f"Email: {participant['email']}")
            print(f"Company: {participant['company']}")
            print(f"Context: {participant['context']}")
            print(f"Total Calls: {participant['total_calls']}")
            print("-" * 40)


def main():
    parser = argparse.ArgumentParser(description="Filter and analyze calls by participants")
    parser.add_argument("--year", type=int, required=True, help="Year to analyze")
    parser.add_argument("--participant", type=str, help="Participant name to filter by")
    parser.add_argument("--email", type=str, help="Participant email to filter by")
    parser.add_argument("--list-participants", action="store_true", help="List all participants")
    parser.add_argument("--context", type=str, choices=["Internal", "External"], help="Filter by context")
    parser.add_argument("--search", type=str, help="Search participants by name, email, or company")
    parser.add_argument("--output-dir", type=str, default="transcripts", help="Base output directory")
    
    args = parser.parse_args()
    
    try:
        filter_tool = ParticipantFilter(args.year, args.output_dir)
        
        if args.list_participants:
            filter_tool.list_participants(args.context)
        elif args.search:
            filter_tool.search_participants(args.search)
        elif args.participant or args.email:
            filter_tool.analyze_participant_calls(args.participant, args.email)
        else:
            print("Please specify an action: --list-participants, --search, or --participant/--email")
            parser.print_help()
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Make sure you have run the transcript downloader first.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 