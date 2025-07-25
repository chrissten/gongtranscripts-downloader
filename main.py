#!/usr/bin/env python3
"""
Gong Transcripts Downloader - CLI Interface

A tool to bulk download call transcripts from Gong.io for analysis and content creation.
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime

import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from config import load_config, REQUIRED_ENV_VARS, OPTIONAL_ENV_VARS
from gong_client import GongSyncClient
from transcript_downloader import TranscriptDownloader

console = Console()


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """
    🚀 Gong Transcripts Downloader
    
    A powerful tool to bulk download call transcripts from Gong.io
    for analysis, content creation, and sales insights.
    """
    pass


@cli.command()
@click.option('--start-date', help='Start date (YYYY-MM-DD). Overrides env variable.')
@click.option('--end-date', help='End date (YYYY-MM-DD). Overrides env variable.')
@click.option('--output-dir', help='Output directory. Overrides env variable.')
@click.option('--dry-run', is_flag=True, help='Show what would be downloaded without actually downloading.')
@click.option('--title-filter', default=None, help="Filter calls by title keywords. Use 'and' for all keywords (e.g., 'identity and demo'), or separate by comma/space for any keyword (e.g., 'empi,demo' or 'empi demo').")
def download(start_date, end_date, output_dir, dry_run, title_filter):
    """Download transcripts from Gong."""
    try:
        # Load configuration
        config = load_config()
        
        # Override with CLI options if provided
        if start_date:
            config.download_start_date = start_date
        if end_date:
            config.download_end_date = end_date
        if output_dir:
            config.output_directory = output_dir
            config.output_path.mkdir(parents=True, exist_ok=True)
        
        if dry_run:
            console.print("[yellow]DRY RUN MODE - No files will be downloaded[/yellow]")
            console.print(f"Date range: {config.download_start_date} to {config.download_end_date}")
            console.print(f"Output directory: {config.output_directory}")
            
            # Test connection and show what would be downloaded
            client = GongSyncClient(config)
            if client.test_connection():
                console.print("[green]✓[/green] API connection successful")
                console.print("[blue]Would proceed with full download...[/blue]")
            return
        
        # Run the actual download
        downloader = TranscriptDownloader(config)
        
        console.print(f"[blue]Starting download for date range: {config.download_start_date} to {config.download_end_date}[/blue]")
        
        # Run async download
        summary = asyncio.run(downloader.download_all_transcripts(title_filter=title_filter))
                
        console.print(f"\n[bold green]✅ Download completed![/bold green]")
        console.print(f"Check your transcripts in: {config.output_directory}")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Download interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
def test():
    """Test API connection and credentials."""
    try:
        config = load_config()
        client = GongSyncClient(config)
        
        console.print("🔧 Testing Gong API connection...")
        console.print(f"Subdomain: {config.gong_subdomain}")
        console.print(f"Base URL: {config.gong_base_url}")
        
        if client.test_connection():
            # Try to get user info for additional validation
            user_info = client.get_user_info()
            if user_info:
                total_users = user_info.get('records', {}).get('totalRecords', 0)
                console.print(f"[green]✓[/green] Found {total_users} users in workspace")
            
            console.print("\n[bold green]✅ All tests passed! You're ready to download transcripts.[/bold green]")
        else:
            console.print("\n[bold red]❌ Connection test failed.[/bold red]")
            console.print("Please check your credentials and try again.")
            sys.exit(1)
            
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        console.print("\nTip: Run 'python main.py setup' for configuration help.")
        sys.exit(1)


@cli.command()
def setup():
    """Show setup instructions and configuration help."""
    
    title = Text("🚀 Gong Transcripts Downloader Setup", style="bold blue")
    
    setup_text = """
This tool downloads call transcripts from Gong.io for analysis and insights.

STEP 1: Get Your Gong API Credentials
1. Log in to your Gong account
2. Go to Settings > API
3. Click "Create" to generate:
   - Access Key
   - Access Key Secret
4. Note your Gong subdomain (e.g., if your URL is https://acme.gong.io, subdomain is "acme")

STEP 2: Set Environment Variables
Create a .env file in this directory with:
"""
    
    # Show required environment variables
    env_example = []
    for var, desc in REQUIRED_ENV_VARS.items():
        env_example.append(f"{var}=your_value_here")
    
    env_example.append("")
    env_example.append("# Optional (with defaults):")
    
    for var, desc in OPTIONAL_ENV_VARS.items():
        env_example.append(f"# {var}=default_value  # {desc}")
    
    setup_text += "\n".join(env_example)
    
    setup_text += """

STEP 3: Test Your Setup
python main.py test

STEP 4: Download Transcripts
python main.py download

For more help, visit: https://github.com/your-repo/gong-transcripts-downloader
"""
    
    console.print(Panel(setup_text, title=title, padding=(1, 2)))
    
    # Check current environment
    console.print("\n[bold yellow]Current Environment Check:[/bold yellow]")
    
    try:
        config = load_config()
        console.print("[green]✓[/green] Configuration loaded successfully")
        console.print(f"  Subdomain: {config.gong_subdomain}")
        console.print(f"  Date range: {config.download_start_date} to {config.download_end_date}")
        console.print(f"  Output directory: {config.output_directory}")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Configuration error: {e}")
        console.print("\nPlease set up your environment variables as shown above.")


@cli.command()
@click.option('--format', 'output_format', 
              type=click.Choice(['json', 'csv', 'txt'], case_sensitive=False),
              default='csv',
              help='Output format for the list')
@click.option('--title-filter', default=None, help="Filter calls by title keywords. Use 'and' for all keywords (e.g., 'identity and demo'), or separate by comma/space for any keyword (e.g., 'empi,demo' or 'empi demo').")
def list_calls(output_format, title_filter):
    """List calls in the configured date range without downloading transcripts."""
    try:
        config = load_config()
        
        console.print(f"[blue]Fetching calls list for {config.download_start_date} to {config.download_end_date}...[/blue]")
        
        async def fetch_calls():
            from gong_client import GongAPIClient
            async with GongAPIClient(config) as client:
                if not await client.test_connection():
                    raise Exception("Failed to connect to Gong API")
                
                calls = await client.get_calls_list(
                    config.download_start_date,
                    config.download_end_date
                )
                return calls
        
        calls = asyncio.run(fetch_calls())
        
        if not calls:
            console.print("[yellow]No calls found in the specified date range.[/yellow]")
            return
        
        # --- Title filtering logic ---
        def filter_calls_by_title(calls, title_filter):
            if not title_filter:
                return calls
            filter_str = title_filter.strip().lower()
            if ' and ' in filter_str:
                keywords = [k.strip() for k in filter_str.split(' and ') if k.strip()]
                def match(title):
                    t = (title or '').lower()
                    return all(kw in t for kw in keywords)
            else:
                # Split by comma or whitespace
                keywords = [k.strip() for k in filter_str.replace(',', ' ').split() if k.strip()]
                def match(title):
                    t = (title or '').lower()
                    return any(kw in t for kw in keywords)
            return [call for call in calls if match(call.get('title', ''))]
        
        calls = filter_calls_by_title(calls, title_filter)
        
        if not calls:
            console.print("[yellow]No calls matched the title filter.[/yellow]")
            return
        
        # Output in requested format
        if output_format.lower() == 'json':
            import json
            output_file = config.output_path / "calls_list.json"
            with open(output_file, 'w') as f:
                json.dump(calls, f, indent=2)
            console.print(f"[green]✓[/green] Calls list saved to {output_file}")
            
        elif output_format.lower() == 'csv':
            import pandas as pd
            
            # Flatten call data for CSV
            rows = []
            for call in calls:
                row = {
                    'id': call.get('id'),
                    'title': call.get('title', ''),
                    'started': call.get('started'),
                    'duration_minutes': int(call.get('duration', 0) // 60000) if call.get('duration') else 0,
                    'direction': call.get('direction', ''),
                    'participants': '; '.join([p.get('name', p.get('emailAddress', '')) for p in call.get('parties', [])]),
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            output_file = config.output_path / "calls_list.csv"
            df.to_csv(output_file, index=False)
            console.print(f"[green]✓[/green] Calls list saved to {output_file}")
            
        else:  # txt format
            output_file = config.output_path / "calls_list.txt"
            with open(output_file, 'w') as f:
                f.write(f"Calls List ({config.download_start_date} to {config.download_end_date})\n")
                f.write("=" * 80 + "\n\n")
                
                for call in calls:
                    f.write(f"ID: {call.get('id')}\n")
                    f.write(f"Title: {call.get('title', 'N/A')}\n")
                    f.write(f"Date: {call.get('started', 'N/A')}\n")
                    f.write(f"Duration: {int(call.get('duration', 0) // 60000)} minutes\n")
                    f.write(f"Participants: {', '.join([p.get('name', p.get('emailAddress', '')) for p in call.get('parties', [])])}\n")
                    f.write("-" * 40 + "\n")
            
            console.print(f"[green]✓[/green] Calls list saved to {output_file}")
        
        console.print(f"\nFound {len(calls)} calls total")
        
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
def estimate():
    """Estimate download scope - how many calls and how long it will take."""
    try:
        console.print("🔍 This will analyze your Gong data to estimate download scope...")
        console.print("💡 Tip: You can also run 'python estimate_download.py' directly for more detailed analysis.")
        
        # Import and run the estimator
        from estimate_download import DownloadEstimator
        
        config = load_config()
        
        async def run_estimate():
            estimator = DownloadEstimator(config)
            analysis = await estimator.analyze_calls_scope(save_call_list=True)
            
            if analysis["total_calls"] > 0:
                estimator.display_analysis(analysis)
                
                # Quick recommendations
                total_calls = analysis["total_calls"]
                if total_calls < 100:
                    console.print("\n✅ [green]Small dataset - ready to download![/green]")
                elif total_calls < 1000:
                    console.print("\n⚠️ [yellow]Medium dataset - plan for several hours[/yellow]")
                else:
                    console.print("\n🚨 [red]Large dataset - will take days to download[/red]")
                    console.print("💡 Consider testing with a smaller date range first")
                
                console.print(f"\n📁 Analysis saved to: {config.output_directory}")
            
            return analysis
        
        summary = asyncio.run(run_estimate())
        
        if summary["total_calls"] == 0:
            console.print("[yellow]No calls found in date range. Check your configuration.[/yellow]")
        
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
def info():
    """Show information about the current configuration and output directory."""
    try:
        config = load_config()
        
        # Show configuration
        console.print(Panel.fit("📊 Current Configuration", style="bold blue"))
        
        console.print(f"[cyan]Gong Settings:[/cyan]")
        console.print(f"  Subdomain: {config.gong_subdomain}")
        console.print(f"  Base URL: {config.gong_base_url}")
        
        console.print(f"\n[cyan]Download Settings:[/cyan]")
        console.print(f"  Date Range: {config.download_start_date} to {config.download_end_date}")
        console.print(f"  Output Directory: {config.output_directory}")
        console.print(f"  Rate Limit: {config.api_rate_limit} calls/second")
        console.print(f"  Max Concurrent: {config.max_concurrent_downloads}")
        
        console.print(f"\n[cyan]Output Formats:[/cyan]")
        console.print(f"  Raw JSON: {'✓' if config.save_raw_json else '✗'}")
        console.print(f"  Formatted Text: {'✓' if config.save_formatted_text else '✗'}")
        console.print(f"  Metadata CSV: {'✓' if config.save_metadata_csv else '✗'}")
        
        # Check output directory
        output_path = Path(config.output_directory)
        if output_path.exists():
            console.print(f"\n[cyan]Output Directory Status:[/cyan]")
            
            # Count existing files
            json_files = len(list((output_path / "raw_json").glob("*.json"))) if (output_path / "raw_json").exists() else 0
            txt_files = len(list((output_path / "transcripts").glob("*.txt"))) if (output_path / "transcripts").exists() else 0
            
            console.print(f"  Existing JSON files: {json_files}")
            console.print(f"  Existing transcript files: {txt_files}")
            
            if (output_path / "download_progress.json").exists():
                console.print(f"  [yellow]Resume file found - previous download can be continued[/yellow]")
        
    except Exception as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        console.print("Run 'python main.py setup' for configuration help.")
        sys.exit(1)


if __name__ == '__main__':
    # Ensure we're using the right event loop policy on Windows
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    cli() 