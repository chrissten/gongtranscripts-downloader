#!/usr/bin/env python3
"""
Gong Download Estimator

This script analyzes your Gong data to estimate:
- How many calls exist in your date range
- How long the download would take
- Storage requirements
- API usage estimates

Use this before running the full download to understand the scope.
"""
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any
import sys

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

from config import load_config
from gong_client import GongAPIClient, GongSyncClient

console = Console()


class DownloadEstimator:
    """Estimates download scope and requirements."""
    
    def __init__(self, config):
        self.config = config
        
    async def analyze_calls_scope(self, save_call_list: bool = True) -> Dict[str, Any]:
        """
        Analyze the scope of calls in the configured date range.
        
        Args:
            save_call_list: Whether to save the call IDs list to a file
            
        Returns:
            Dictionary with analysis results
        """
        console.print(Panel.fit("🔍 Analyzing Gong Calls Scope", style="bold blue"))
        
        console.print(f"📅 Date Range: {self.config.download_start_date} to {self.config.download_end_date}")
        console.print(f"🏢 Gong Subdomain: {self.config.gong_subdomain}")
        
        async with GongAPIClient(self.config) as client:
            # Test connection
            if not await client.test_connection():
                raise Exception("Failed to connect to Gong API. Check your credentials.")
            
            # Get all calls in date range
            console.print("\n🔍 Discovering all calls in date range...")
            calls = await client.get_calls_list(
                self.config.download_start_date,
                self.config.download_end_date
            )
            
            if not calls:
                console.print("[yellow]No calls found in the specified date range.[/yellow]")
                return {"total_calls": 0}
            
            # Analyze the calls
            analysis = self._analyze_calls_data(calls)
            
            # Save call list if requested
            if save_call_list:
                await self._save_call_list(calls, analysis)
            
            return analysis
    
    def _analyze_calls_data(self, calls: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze the calls data and extract insights."""
        console.print("\n📊 Analyzing call data...")
        
        analysis = {
            "total_calls": len(calls),
            "call_ids": [call.get('id') for call in calls if call.get('id')],
            "date_range": {
                "start": self.config.download_start_date,
                "end": self.config.download_end_date
            }
        }
        
        # Extract dates and durations
        call_dates = []
        durations = []
        participants_count = []
        directions = []
        
        for call in calls:
            # Extract date
            started = call.get('started')
            if started:
                try:
                    dt = datetime.fromisoformat(started.replace('Z', '+00:00'))
                    call_dates.append(dt.date())
                except:
                    pass
            
            # Extract duration (convert from milliseconds to minutes)
            duration = call.get('duration', 0)
            if duration:
                durations.append(duration / 60000)  # Convert to minutes
            
            # Count participants
            parties = call.get('parties', [])
            participants_count.append(len(parties))
            
            # Direction
            direction = call.get('direction', 'Unknown')
            directions.append(direction)
        
        # Calculate statistics
        if call_dates:
            analysis["date_stats"] = {
                "earliest": min(call_dates).isoformat(),
                "latest": max(call_dates).isoformat(),
                "span_days": (max(call_dates) - min(call_dates)).days
            }
        
        if durations:
            analysis["duration_stats"] = {
                "total_minutes": sum(durations),
                "total_hours": sum(durations) / 60,
                "average_minutes": sum(durations) / len(durations),
                "shortest_minutes": min(durations),
                "longest_minutes": max(durations),
                "calls_over_60_min": len([d for d in durations if d > 60])
            }
        
        if participants_count:
            analysis["participant_stats"] = {
                "average_participants": sum(participants_count) / len(participants_count),
                "max_participants": max(participants_count),
                "min_participants": min(participants_count)
            }
        
        # Count directions
        if directions:
            from collections import Counter
            direction_counts = Counter(directions)
            analysis["direction_stats"] = dict(direction_counts)
        
        # Calculate estimates
        analysis["estimates"] = self._calculate_estimates(analysis)
        
        return analysis
    
    def _calculate_estimates(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate download time and resource estimates."""
        total_calls = analysis["total_calls"]
        
        # API rate limits (conservative estimates)
        calls_per_second = self.config.api_rate_limit  # Usually 2.5
        calls_per_day_limit = 10000  # Gong's daily limit
        
        # Estimate API calls needed
        # 1. Calls list API calls (already done, but count for future runs)
        calls_list_requests = max(1, total_calls // 100)  # 100 calls per request
        
        # 2. Transcript API calls 
        transcript_requests = max(1, total_calls // 100)  # 100 transcripts per request
        
        total_api_calls = calls_list_requests + transcript_requests
        
        # Time estimates
        time_seconds = total_api_calls / calls_per_second
        time_minutes = time_seconds / 60
        time_hours = time_minutes / 60
        
        # Daily limit considerations
        days_by_rate_limit = time_hours / 24  # If we could run 24/7 at rate limit
        days_by_daily_limit = total_api_calls / calls_per_day_limit  # Daily API limit
        
        # Conservative estimate (accounts for processing time, retries, etc.)
        estimated_days = max(days_by_rate_limit, days_by_daily_limit) * 1.5  # 50% buffer
        
        # Storage estimates (rough)
        avg_transcript_size_kb = 50  # Conservative estimate per transcript
        total_storage_mb = (total_calls * avg_transcript_size_kb) / 1024
        
        return {
            "api_calls_needed": total_api_calls,
            "calls_list_requests": calls_list_requests,
            "transcript_requests": transcript_requests,
            "time_estimates": {
                "seconds": round(time_seconds),
                "minutes": round(time_minutes, 1),
                "hours": round(time_hours, 2),
                "estimated_days": round(estimated_days, 2)
            },
            "daily_limits": {
                "days_by_rate_limit": round(days_by_rate_limit, 2),
                "days_by_daily_limit": round(days_by_daily_limit, 2),
                "recommended_days": round(estimated_days, 2)
            },
            "storage_estimates": {
                "transcripts_mb": round(total_storage_mb, 1),
                "total_with_metadata_mb": round(total_storage_mb * 2, 1)  # Include JSON, CSV, etc.
            }
        }
    
    async def _save_call_list(self, calls: List[Dict[str, Any]], analysis: Dict[str, Any]):
        """Save the call list and analysis to files."""
        output_dir = Path(self.config.output_directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save call IDs list
        call_ids = [call.get('id') for call in calls if call.get('id')]
        with open(output_dir / "call_ids_list.txt", 'w') as f:
            for call_id in call_ids:
                f.write(f"{call_id}\n")
        
        # Save detailed analysis
        analysis_file = output_dir / "download_analysis.json"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        # Save call metadata summary
        call_summary = []
        for call in calls:
            started = call.get('started', '')
            duration_minutes = 0
            if call.get('duration'):
                duration_minutes = call.get('duration') / 60000
            
            call_summary.append({
                'id': call.get('id'),
                'started': started,
                'duration_minutes': round(duration_minutes, 1),
                'title': call.get('title', ''),
                'direction': call.get('direction', ''),
                'participant_count': len(call.get('parties', []))
            })
        
        import pandas as pd
        df = pd.DataFrame(call_summary)
        df.to_csv(output_dir / "calls_summary.csv", index=False)
        
        console.print(f"\n💾 Analysis files saved to: {output_dir}")
        console.print(f"  📄 call_ids_list.txt - List of all call IDs")
        console.print(f"  📊 calls_summary.csv - Call metadata summary")
        console.print(f"  🔍 download_analysis.json - Complete analysis")
    
    def display_analysis(self, analysis: Dict[str, Any]):
        """Display the analysis results in a nice format."""
        
        # Overview table
        overview_table = Table(title="📊 Download Scope Overview")
        overview_table.add_column("Metric", style="cyan")
        overview_table.add_column("Value", style="green")
        
        overview_table.add_row("Total Calls", f"{analysis['total_calls']:,}")
        
        if "date_stats" in analysis:
            date_stats = analysis["date_stats"]
            overview_table.add_row("Date Range", f"{date_stats['earliest']} to {date_stats['latest']}")
            overview_table.add_row("Span", f"{date_stats['span_days']:,} days")
        
        if "duration_stats" in analysis:
            duration_stats = analysis["duration_stats"]
            overview_table.add_row("Total Call Time", f"{duration_stats['total_hours']:.1f} hours")
            overview_table.add_row("Average Call Duration", f"{duration_stats['average_minutes']:.1f} minutes")
            overview_table.add_row("Longest Call", f"{duration_stats['longest_minutes']:.1f} minutes")
            overview_table.add_row("Calls Over 60 Min", f"{duration_stats['calls_over_60_min']:,}")
        
        console.print(overview_table)
        
        # Download estimates
        if "estimates" in analysis:
            estimates = analysis["estimates"]
            
            estimate_table = Table(title="⏱️ Download Time Estimates")
            estimate_table.add_column("Scenario", style="cyan")
            estimate_table.add_column("Estimate", style="yellow")
            estimate_table.add_column("Notes", style="dim")
            
            time_est = estimates["time_estimates"]
            estimate_table.add_row(
                "Minimum Time", 
                f"{time_est['hours']:.1f} hours",
                "Pure API time at rate limit"
            )
            estimate_table.add_row(
                "Realistic Estimate", 
                f"{time_est['estimated_days']:.1f} days",
                "Includes processing, retries, buffers"
            )
            
            daily_limits = estimates["daily_limits"]
            estimate_table.add_row(
                "By Rate Limit", 
                f"{daily_limits['days_by_rate_limit']:.1f} days",
                f"At {self.config.api_rate_limit} calls/second"
            )
            estimate_table.add_row(
                "By Daily Limit", 
                f"{daily_limits['days_by_daily_limit']:.1f} days",
                "Gong's 10,000 calls/day limit"
            )
            
            console.print(estimate_table)
            
            # Storage estimates
            storage_table = Table(title="💾 Storage Estimates")
            storage_table.add_column("Type", style="cyan")
            storage_table.add_column("Size", style="green")
            
            storage = estimates["storage_estimates"]
            storage_table.add_row("Transcripts Only", f"{storage['transcripts_mb']:.1f} MB")
            storage_table.add_row("Total (with metadata)", f"{storage['total_with_metadata_mb']:.1f} MB")
            
            console.print(storage_table)
            
            # API usage
            api_table = Table(title="🔌 API Usage")
            api_table.add_column("Operation", style="cyan")
            api_table.add_column("API Calls", style="green")
            
            api_table.add_row("Calls Discovery", f"{estimates['calls_list_requests']:,}")
            api_table.add_row("Transcript Download", f"{estimates['transcript_requests']:,}")
            api_table.add_row("Total API Calls", f"{estimates['api_calls_needed']:,}")
            
            console.print(api_table)
        
        # Direction breakdown
        if "direction_stats" in analysis:
            direction_table = Table(title="📞 Call Direction Breakdown")
            direction_table.add_column("Direction", style="cyan")
            direction_table.add_column("Count", style="green")
            direction_table.add_column("Percentage", style="yellow")
            
            total = analysis["total_calls"]
            for direction, count in analysis["direction_stats"].items():
                percentage = (count / total) * 100
                direction_table.add_row(direction, f"{count:,}", f"{percentage:.1f}%")
            
            console.print(direction_table)


async def main():
    """Main function to run the download estimation."""
    console.print(Panel.fit("🔍 Gong Download Scope Estimator", style="bold blue"))
    
    try:
        # Load configuration
        config = load_config()
        
        console.print(f"\n⚙️ Configuration:")
        console.print(f"  📅 Date Range: {config.download_start_date} to {config.download_end_date}")
        console.print(f"  🏢 Subdomain: {config.gong_subdomain}")
        console.print(f"  📁 Output Directory: {config.output_directory}")
        console.print(f"  ⚡ Rate Limit: {config.api_rate_limit} calls/second")
        
        # Ask user if they want to proceed
        console.print(f"\n[yellow]This will query your Gong API to count calls in the date range.[/yellow]")
        console.print(f"[yellow]This uses minimal API calls and is safe to run.[/yellow]")
        
        proceed = input("\nProceed with analysis? (y/N): ").lower().strip()
        if proceed not in ['y', 'yes']:
            console.print("[yellow]Analysis cancelled.[/yellow]")
            return
        
        # Run the analysis
        estimator = DownloadEstimator(config)
        analysis = await estimator.analyze_calls_scope(save_call_list=True)
        
        if analysis["total_calls"] == 0:
            console.print("\n[yellow]No calls found in the specified date range.[/yellow]")
            console.print("Try adjusting your date range in the .env file.")
            return
        
        # Display results
        console.print("\n" + "="*80)
        estimator.display_analysis(analysis)
        
        # Recommendations
        console.print(Panel.fit("💡 Recommendations", style="bold green"))
        
        total_calls = analysis["total_calls"]
        estimates = analysis.get("estimates", {})
        
        if total_calls < 100:
            console.print("✅ Small dataset - should download quickly (< 1 hour)")
        elif total_calls < 1000:
            console.print("⚠️ Medium dataset - will take several hours to download")
            console.print("💡 Consider running overnight or during off-hours")
        else:
            console.print("🚨 Large dataset - will take days to download")
            console.print("💡 Strongly recommend:")
            console.print("   • Start with a smaller date range for testing")
            console.print("   • Run during off-peak hours")
            console.print("   • Consider splitting into monthly chunks")
            
            if estimates and estimates.get("daily_limits", {}).get("days_by_daily_limit", 0) > 1:
                console.print("   • Contact Gong support to increase daily API limits")
        
        # Next steps
        console.print("\n[bold cyan]Next Steps:[/bold cyan]")
        console.print("1. Review the analysis files saved to your output directory")
        console.print("2. Adjust date range if needed (in .env file)")
        console.print("3. Run: python main.py download --dry-run")
        console.print("4. Run: python main.py download")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Analysis interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        console.print("\nTip: Run 'python main.py setup' for configuration help.")
        sys.exit(1)


if __name__ == "__main__":
    # Ensure we're using the right event loop policy on Windows
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(main()) 