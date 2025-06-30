# 🚀 Gong Transcripts Downloader

A Python tool to bulk download Gong.io call transcripts for analysis and content creation, with enhanced participant tracking and filtering capabilities.

## ✨ Features

- **Bulk Download**: Download transcripts for multiple calls in a date range
- **Resume Capability**: Resume interrupted downloads
- **Multiple Formats**: Save data in JSON, formatted text, and CSV formats
- **Year-based Organization**: Automatically organize files by year
- **Enhanced Participant Tracking**: Detailed participant information and statistics
- **Participant Filtering**: Easy filtering and analysis by specific participants
- **Multiple Organization Methods**: By date, speaker, and participant

## 🎯 Use Cases

Perfect for extracting insights from your sales conversations:

- **Content Creation**: Mine transcripts for LinkedIn posts, blog content, case studies
- **Sales Training**: Identify best practices and common objections
- **Product Insights**: Discover feature requests and customer pain points
- **Competitive Intelligence**: Understand competitor mentions and positioning
- **Sales Performance**: Analyze team performance and coaching opportunities

## 📋 Prerequisites

- Python 3.8 or higher
- Gong.io account with API access
- Technical administrator permissions in Gong (to access API settings)

## 🛠️ Installation

1. **Clone or download this project**:
   ```bash
   git clone <your-repo-url>
   cd gongtranscripts-downloader
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Get your Gong API credentials**:
   - Log in to your Gong account
   - Go to **Settings** > **API**
   - Click **"Create"** to generate:
     - Access Key
     - Access Key Secret
   - Note your Gong subdomain (e.g., if your URL is `https://acme.gong.io`, subdomain is `acme`)

4. **Create a `.env` file** in the project directory:
   ```bash
   # Required - Gong API Credentials
   GONG_ACCESS_KEY=your_access_key_here
   GONG_ACCESS_KEY_SECRET=your_access_key_secret_here
   GONG_SUBDOMAIN=your_subdomain_here

   # Optional - Download Configuration
   DOWNLOAD_START_DATE=2022-01-01
   DOWNLOAD_END_DATE=2024-12-31
   OUTPUT_DIRECTORY=./transcripts
   MAX_CONCURRENT_DOWNLOADS=3
   API_RATE_LIMIT=2.5
   ```

5. **Test your setup**:
   ```bash
   python main.py test
   ```

## 🚀 Quick Start

### Basic Usage

```bash
# First: Estimate how many calls and download time
python main.py estimate

# Test API connection
python main.py test

# Download all transcripts in configured date range
python main.py download

# Show setup instructions
python main.py setup

# Show current configuration
python main.py info
```

### Advanced Usage

```bash
# Download with custom date range
python main.py download --start-date 2023-01-01 --end-date 2023-12-31

# Dry run to see what would be downloaded
python main.py download --dry-run

# List calls without downloading transcripts
python main.py list-calls --format csv

# Custom output directory
python main.py download --output-dir /path/to/custom/directory
```

## 📁 Output Structure

The tool creates an organized directory structure:

```
transcripts/
├── raw_json/           # Raw API responses
│   ├── call_123.json
│   ├── call_456.json
│   └── all_data.json   # Consolidated file
├── transcripts/        # Formatted text transcripts
│   ├── transcript_123_2023-01-15.txt
│   └── transcript_456_2023-01-16.txt
├── by_date/           # Organized by date
│   ├── 2023-01-15/
│   └── 2023-01-16/
├── calls_metadata.csv  # Summary of all calls
├── summary_statistics.csv
└── logs/              # Download logs
    └── download_20240101_120000.log
```

## 📊 File Formats

### 1. Raw JSON (`raw_json/`)
Complete API responses with all metadata and transcript data.

### 2. Formatted Text (`transcripts/`)
Human-readable transcripts with:
- Call metadata (date, time, participants)
- Timestamped conversation
- Speaker identification

Example:
```
================================================================================
CALL TRANSCRIPT
================================================================================
Call ID: 123456789
Date: 2023-01-15
Time: 14:30
Duration: 45 minutes
Title: Discovery Call - Acme Corp
Participants: John Sales, Jane Prospect
--------------------------------------------------------------------------------

[00:01] John Sales: Hi Jane, thanks for taking the time today...
[00:45] Jane Prospect: Thanks John, excited to learn more about your solution...
```

### 3. Metadata CSV (`calls_metadata.csv`)
Structured data for analysis:
- Call IDs, dates, durations
- Participant lists (internal/external)
- CRM associations
- Transcript availability status

## ⚙️ Configuration Options

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GONG_ACCESS_KEY` | ✅ | - | Your Gong API Access Key |
| `GONG_ACCESS_KEY_SECRET` | ✅ | - | Your Gong API Access Key Secret |
| `GONG_SUBDOMAIN` | ✅ | - | Your Gong subdomain |
| `DOWNLOAD_START_DATE` | ❌ | `2022-01-01` | Start date (YYYY-MM-DD) |
| `DOWNLOAD_END_DATE` | ❌ | `2024-12-31` | End date (YYYY-MM-DD) |
| `OUTPUT_DIRECTORY` | ❌ | `./transcripts` | Output directory |
| `MAX_CONCURRENT_DOWNLOADS` | ❌ | `3` | Concurrent API calls |
| `API_RATE_LIMIT` | ❌ | `2.5` | API calls per second |
| `SAVE_RAW_JSON` | ❌ | `True` | Save raw JSON files |
| `SAVE_FORMATTED_TEXT` | ❌ | `True` | Save formatted transcripts |
| `SAVE_METADATA_CSV` | ❌ | `True` | Save metadata CSV |

## 🔧 Command Reference

### `python main.py download`
Downloads transcripts from Gong.

**Options:**
- `--start-date YYYY-MM-DD` - Override start date
- `--end-date YYYY-MM-DD` - Override end date  
- `--output-dir PATH` - Override output directory
- `--dry-run` - Show what would be downloaded without downloading

### `python main.py test`
Tests API connection and credentials.

### `python main.py setup`
Shows detailed setup instructions and checks current configuration.

### `python main.py list-calls`
Lists calls in date range without downloading transcripts.

**Options:**
- `--format [json|csv|txt]` - Output format (default: csv)

### `python main.py info`
Shows current configuration and output directory status.

## 🛡️ Rate Limiting & API Limits

Gong API has default limits:
- **3 calls per second**
- **10,000 calls per day**

The tool respects these limits with:
- Configurable rate limiting (`API_RATE_LIMIT`)
- Exponential backoff on rate limit errors
- Resume capability for large downloads

For higher limits, contact Gong support.

## 🔄 Resume Capability

If a download is interrupted:
1. The tool saves progress automatically
2. Run `python main.py download` again
3. It will resume from where it left off
4. Progress file is cleaned up after successful completion

## 📈 Performance Optimization

For large datasets (thousands of calls):

1. **Adjust rate limiting**:
   ```bash
   export API_RATE_LIMIT=2.8  # Just under the 3/second limit
   ```

2. **Use concurrent downloads**:
   ```bash
   export MAX_CONCURRENT_DOWNLOADS=5
   ```

3. **Split date ranges** for very large datasets:
   ```bash
   python main.py download --start-date 2022-01-01 --end-date 2022-06-30
   python main.py download --start-date 2022-07-01 --end-date 2022-12-31
   ```

## 🐛 Troubleshooting

### Common Issues

**1. "Configuration error" when running commands**
- Check that your `.env` file exists and has the required variables
- Run `python main.py setup` to see what's missing

**2. "API connection failed"**
- Verify your credentials in Gong Settings > API
- Check that your subdomain is correct
- Ensure you have technical administrator permissions

**3. "Rate limited" errors**
- Reduce `API_RATE_LIMIT` in your `.env` file
- Contact Gong to increase your rate limits

**4. Transcripts missing for some calls**
- Some calls may not have transcripts available
- Check the `calls_metadata.csv` file for `has_transcript` status
- Gong only transcribes calls that meet certain criteria

**5. Large downloads timing out**
- Use the resume capability - restart the download
- Split into smaller date ranges
- Increase `API_TIMEOUT` if needed

### Getting Help

1. Check the logs in `transcripts/logs/` for detailed error information
2. Run `python main.py info` to check your configuration
3. Use `--dry-run` to test without downloading

## 📊 Analysis Examples

Once you have your transcripts, here are some analysis ideas:

### 1. Content Mining
```python
# Find common customer pain points
import pandas as pd
df = pd.read_csv('transcripts/calls_metadata.csv')
# Analyze external participant questions
```

### 2. Sales Performance
```python
# Analyze call duration vs success metrics
# Compare internal vs external talk time
# Identify top-performing sales reps
```

### 3. Product Insights
```python
# Search transcripts for feature requests
# Find competitor mentions
# Analyze pricing discussions
```

## 🔒 Security & Privacy

- **Credentials**: Store API credentials securely in `.env` file (not in code)
- **Data**: Downloaded transcripts contain sensitive customer conversations
- **Access**: Ensure appropriate access controls on output directories
- **Compliance**: Follow your company's data retention and privacy policies

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🔗 Resources

- [Gong API Documentation](https://app.gong.io/settings/api/documentation)
- [Gong Developer Community](https://visioneers.gong.io/developers-79)
- [Rate Limiting Best Practices](https://help.gong.io/docs/what-the-gong-api-provides)

---

**Happy transcript mining! 🎉**

*Transform your sales conversations into actionable insights.*

## Enhanced Participant Data Collection

The tool now collects comprehensive participant information for future filtering and analysis:

### Participant Data Captured:
- **Basic Info**: Name, email, context (Internal/External)
- **Professional Info**: Role, company, title
- **Call Statistics**: Total calls, duration, host/organizer counts
- **Temporal Data**: First/last seen dates, call history
- **Speaker Mapping**: Speaker IDs for transcript correlation

### Generated Files:
- `participants.csv`: Complete participant profiles with statistics
- `participant_summary.csv`: Summary statistics across all participants
- Enhanced `calls_metadata.csv`: Individual participant columns for easy filtering
- `by_participant/`: Directory structure with transcripts organized by participant

## Future Use Cases

With the enhanced participant data, you can easily:

1. **Filter by Sales Rep**: Find all calls by specific sales representatives
2. **Performance Analysis**: Compare call statistics across team members
3. **Customer Analysis**: Track interactions with specific customers
4. **Team Collaboration**: Analyze internal vs external participation
5. **Content Creation**: Extract insights from top performers' calls
6. **Training**: Use successful calls as training materials

## Example Analysis Workflows

### Sales Rep Performance Analysis
```bash
# Get Greg's call statistics
python participant_filter.py --year 2025 --participant "Greg"

# Compare with other sales reps
python participant_filter.py --year 2025 --list-participants --context Internal
```

### Customer Interaction Analysis
```bash
# Find calls with specific customers
python participant_filter.py --year 2025 --search "Acme Corp"
```

### Team Collaboration Analysis
```bash
# See who works together most
python participant_filter.py --year 2025 --list-participants
```

## Troubleshooting

### Common Issues

1. **API Rate Limits**: The tool includes built-in rate limiting and retry logic
2. **Large Date Ranges**: Consider downloading in smaller chunks for very large ranges
3. **Missing Transcripts**: Some calls may not have transcripts available
4. **Participant Matching**: Use email addresses for more precise participant filtering

### Resume Downloads

If a download is interrupted, simply run the tool again. It will automatically resume from where it left off.

## Requirements

- Python 3.8+
- Gong API access
- Required packages (see `requirements.txt`)

## License

[Your License Here] 