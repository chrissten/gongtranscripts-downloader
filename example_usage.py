#!/usr/bin/env python3
"""
Example usage of the Gong Transcripts Downloader

This script demonstrates how to use the downloader programmatically
and shows some basic analysis examples.
"""
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from config import load_config
from transcript_downloader import TranscriptDownloader
from gong_client import GongSyncClient


def example_basic_download():
    """Basic example of downloading transcripts."""
    print("🚀 Basic Download Example")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    
    # Test connection first
    client = GongSyncClient(config)
    if not client.test_connection():
        print("❌ API connection failed. Check your credentials.")
        return
    
    print("✅ API connection successful")
    
    # Override date range for this example (last 30 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    config.download_start_date = start_date.strftime('%Y-%m-%d')
    config.download_end_date = end_date.strftime('%Y-%m-%d')
    
    print(f"📅 Downloading calls from {config.download_start_date} to {config.download_end_date}")
    
    # Create downloader and run
    downloader = TranscriptDownloader(config)
    
    # Run the download
    summary = asyncio.run(downloader.download_all_transcripts())
    
    print(f"\n📊 Download Summary:")
    print(f"  Total calls: {summary['total_calls']}")
    print(f"  Downloaded transcripts: {summary['downloaded_transcripts']}")
    print(f"  Success rate: {summary['success_rate']:.1%}")
    print(f"  Duration: {summary['download_duration']}")
    

def example_analysis():
    """Example analysis of downloaded transcripts."""
    print("\n🔍 Analysis Examples")
    print("=" * 50)
    
    config = load_config()
    metadata_file = Path(config.output_directory) / "calls_metadata.csv"
    
    if not metadata_file.exists():
        print("❌ No metadata file found. Run a download first.")
        return
    
    # Load the metadata
    df = pd.read_csv(metadata_file)
    
    print(f"📊 Loaded data for {len(df)} calls")
    
    # Basic statistics
    print(f"\n📈 Basic Statistics:")
    print(f"  Calls with transcripts: {df['has_transcript'].sum()}")
    print(f"  Average call duration: {df['duration_minutes'].mean():.1f} minutes")
    print(f"  Total call time: {df['duration_minutes'].sum():.0f} minutes")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Participant analysis
    print(f"\n👥 Participant Analysis:")
    
    # Count internal participants
    all_internal = []
    for participants in df['internal_participants'].dropna():
        all_internal.extend([p.strip() for p in participants.split(';') if p.strip()])
    
    internal_counts = pd.Series(all_internal).value_counts()
    print(f"  Top 5 internal participants:")
    for name, count in internal_counts.head().items():
        print(f"    {name}: {count} calls")
    
    # Daily call volume
    print(f"\n📅 Daily Call Volume:")
    daily_calls = df.groupby('date').size().sort_index()
    print(f"  Busiest day: {daily_calls.idxmax()} ({daily_calls.max()} calls)")
    print(f"  Average calls per day: {daily_calls.mean():.1f}")
    
    # Duration analysis
    print(f"\n⏱️ Duration Analysis:")
    print(f"  Shortest call: {df['duration_minutes'].min()} minutes")
    print(f"  Longest call: {df['duration_minutes'].max()} minutes")
    print(f"  Calls over 60 minutes: {(df['duration_minutes'] > 60).sum()}")


def example_content_search():
    """Example of searching through transcript content."""
    print("\n🔍 Content Search Example")
    print("=" * 50)
    
    config = load_config()
    transcripts_dir = Path(config.output_directory) / "transcripts"
    
    if not transcripts_dir.exists():
        print("❌ No transcripts directory found. Run a download first.")
        return
    
    # Keywords to search for
    keywords = ["pricing", "competitor", "feature", "timeline", "budget"]
    
    print(f"🔎 Searching for keywords: {', '.join(keywords)}")
    
    results = {}
    transcript_files = list(transcripts_dir.glob("*.txt"))
    
    for keyword in keywords:
        results[keyword] = []
        
        for file_path in transcript_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().lower()
                    if keyword.lower() in content:
                        results[keyword].append(file_path.name)
            except Exception as e:
                print(f"⚠️ Error reading {file_path}: {e}")
    
    # Display results
    for keyword, files in results.items():
        print(f"\n📋 '{keyword}' found in {len(files)} transcripts:")
        for file_name in files[:5]:  # Show first 5
            print(f"  - {file_name}")
        if len(files) > 5:
            print(f"  ... and {len(files) - 5} more")


def example_export_for_analysis():
    """Example of preparing data for external analysis tools."""
    print("\n📊 Export for Analysis Example")
    print("=" * 50)
    
    config = load_config()
    output_dir = Path(config.output_directory)
    metadata_file = output_dir / "calls_metadata.csv"
    
    if not metadata_file.exists():
        print("❌ No metadata file found. Run a download first.")
        return
    
    df = pd.read_csv(metadata_file)
    
    # Create analysis-ready datasets
    
    # 1. Daily summary
    daily_summary = df.groupby('date').agg({
        'call_id': 'count',
        'duration_minutes': ['sum', 'mean'],
        'has_transcript': 'sum'
    }).round(2)
    
    daily_summary.columns = ['total_calls', 'total_duration', 'avg_duration', 'calls_with_transcripts']
    daily_summary.to_csv(output_dir / "daily_summary.csv")
    print("✅ Created daily_summary.csv")
    
    # 2. Participant summary
    if not df['internal_participants'].isna().all():
        participant_data = []
        for _, row in df.iterrows():
            if pd.notna(row['internal_participants']):
                for participant in row['internal_participants'].split(';'):
                    participant = participant.strip()
                    if participant:
                        participant_data.append({
                            'participant': participant,
                            'call_id': row['call_id'],
                            'date': row['date'],
                            'duration_minutes': row['duration_minutes'],
                            'has_transcript': row['has_transcript']
                        })
        
        if participant_data:
            participant_df = pd.DataFrame(participant_data)
            participant_summary = participant_df.groupby('participant').agg({
                'call_id': 'count',
                'duration_minutes': ['sum', 'mean'],
                'has_transcript': 'sum'
            }).round(2)
            
            participant_summary.columns = ['total_calls', 'total_duration', 'avg_duration', 'calls_with_transcripts']
            participant_summary.to_csv(output_dir / "participant_summary.csv")
            print("✅ Created participant_summary.csv")
    
    # 3. Transcript availability report
    transcript_stats = {
        'total_calls': len(df),
        'calls_with_transcripts': df['has_transcript'].sum(),
        'transcript_rate': df['has_transcript'].mean(),
        'avg_transcript_length': df[df['has_transcript']]['transcript_length'].mean(),
        'total_transcript_chars': df['transcript_length'].sum()
    }
    
    pd.DataFrame([transcript_stats]).to_csv(output_dir / "transcript_stats.csv", index=False)
    print("✅ Created transcript_stats.csv")
    
    print(f"\n📈 Analysis files saved to: {output_dir}")
    print("These files can be imported into Excel, Tableau, or other analysis tools.")


if __name__ == "__main__":
    print("🚀 Gong Transcripts Downloader - Example Usage")
    print("=" * 60)
    
    try:
        # Uncomment the examples you want to run:
        
        # 1. Basic download (downloads last 30 days)
        # example_basic_download()
        
        # 2. Analyze existing data
        example_analysis()
        
        # 3. Search transcript content
        example_content_search()
        
        # 4. Export for external analysis
        example_export_for_analysis()
        
        print("\n✅ Example usage completed!")
        print("\nTo run a full download, use: python main.py download")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have:")
        print("1. Set up your .env file with Gong credentials")
        print("2. Installed dependencies: pip install -r requirements.txt")
        print("3. Run a download first if testing analysis functions") 