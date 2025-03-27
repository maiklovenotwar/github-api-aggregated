# GitHub Global Contribution Analysis

A high-performance ETL system for analyzing global GitHub contribution patterns through geographical data enrichment and analysis. This system uses a hybrid approach combining the GitHub API and BigQuery/GitHub Archive for comprehensive insights.

## 🌍 Project Focus

This project focuses on analyzing geographical patterns in open-source contributions by:

1. **Collecting comprehensive GitHub repository data** using efficient collection strategies
2. **Enriching data with geographical information** through geocoding of user and organization locations
3. **Analyzing contribution patterns across countries and regions** to identify global trends
4. **Providing insights into open-source participation by region** over time

## 🌟 Key Features

- **Hybrid Data Collection**: Combines GitHub API (for detailed metadata) and BigQuery/GitHub Archive (for historical events)
- **Geographical Enrichment**: Extracts and geocodes location data from contributors and organizations
- **Efficient Collection Strategies**:
  - Star-based collection for popular repositories (configurable star ranges)
  - Time-period collection for historical analysis
- **Performance Optimizations**:
  - Multi-threaded parallel processing with configurable worker count
  - Token pool management for handling GitHub API rate limits
  - Intelligent caching system for reducing API calls
  - Progress tracking with resume capability
- **Comprehensive Analysis**:
  - Country-level contribution aggregation
  - Organization-based geographical analysis
  - Yearly statistics for tracking trends over time

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- GitHub API token(s)
- [Optional] Google Cloud account for BigQuery access
- SQLAlchemy-compatible database (SQLite by default)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/github-global-contributions.git
cd github-global-contributions

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.template .env
# Edit .env file to add your GitHub API token(s) and other configuration
```

### Basic Usage

```bash
# Collect repositories using star ranges strategy
python -m src.github_database.main --mode collect --collection-strategy star-ranges --min-stars 50 --limit 1000

# Collect repositories using time period strategy
python -m src.github_database.main --mode collect --collection-strategy time-period --start-year 2015 --end-year 2023 --limit 500

# Enrich collected data with geographical information
python -m src.github_database.main --mode enrich

# Aggregate data for analysis
python -m src.github_database.main --mode aggregate --start-year 2015 --end-year 2023

# Export aggregated data to CSV files
python -m src.github_database.main --mode export --output-dir ./data/exports
```

### Advanced Usage

```bash
# Parallel collection with multiple tokens and custom star ranges
python -m src.github_database.main --mode collect --collection-strategy star-ranges --parallel --workers 10 --tokens TOKEN1 TOKEN2 TOKEN3 --star-ranges "[(10000, None), (5000, 9999), (1000, 4999), (500, 999)]" --limit 2000

# Update existing geographical data
python -m src.github_database.main --mode enrich --update-existing
```

## 🏗 Project Structure

```
github-api-aggregated/
├── src/
│   └── github_database/
│       ├── api/                    # API Integration
│       │   ├── github_api.py       # GitHub API client with rate limiting
│       │   ├── token_pool.py       # Token management for API requests
│       │   └── bigquery_api.py     # BigQuery client for GitHub Archive
│       │
│       ├── database/               # Database models and management
│       │   ├── database.py         # SQLAlchemy models (Repository, Contributor, Organization)
│       │   └── migrations/         # Database migrations
│       │
│       ├── aggregation/            # Data aggregation
│       │   └── data_aggregator.py  # Repository data aggregation
│       │
│       ├── config.py               # Configuration management
│       ├── collection_strategies.py # Repository collection strategies
│       ├── etl_orchestrator.py     # ETL process coordination
│       ├── optimized_collector.py  # Optimized parallel data collection
│       └── main.py                 # Main application entry point
│
├── .env.template                  # Environment variables template
├── requirements.txt               # Python dependencies
└── README.md                      # Project documentation
```

## 🔑 Core Components

### Collection Strategies
- **StarRangeCollectionStrategy**: Collects repositories based on configurable star count ranges
- **TimePeriodCollectionStrategy**: Collects repositories based on creation date ranges

### ETL Orchestrator
- Coordinates the entire data collection and processing workflow
- Manages geocoding and geographical enrichment
- Handles database interactions and transaction management

### Optimized Collector
- Implements parallel processing for efficient data collection
- Uses thread pooling for concurrent repository processing
- Manages token usage and rate limiting

### Geographical Enrichment
- Geocodes user and organization locations to extract country and region information
- Maintains a geocoding cache to reduce redundant API calls
- Enables geographical aggregation and analysis

## 📊 Data Analysis Capabilities

The primary focus of this project is to analyze:

1. **Global Distribution of Open-Source Contributors**
   - Map contributions by country and region
   - Identify emerging open-source hubs

2. **Organization Geographic Influence**
   - Analyze where organizations' contributors are located
   - Track geographical spread of organizational influence

3. **Regional Contribution Patterns**
   - Compare contribution volumes across different regions
   - Analyze contribution trends over time

4. **Cross-Border Collaboration**
   - Identify collaboration patterns between contributors from different countries
   - Analyze repository diversity by contributor locations

## 📋 Database Schema and Variables

This section provides a detailed overview of all data variables collected and stored in the database.

### Repository Data

| Variable | Type | Description |
|----------|------|-------------|
| `id` | Integer | Unique GitHub repository identifier |
| `name` | String | Repository name (without owner) |
| `full_name` | String | Complete repository name in format `owner/repo` |
| `owner_id` | Integer | ID of the repository owner (contributor) |
| `organization_id` | Integer | ID of the organization if repository belongs to one |
| `description` | Text | Repository description text |
| `homepage` | String | URL to the project homepage |
| `language` | String | Primary programming language used |
| `private` | Boolean | Whether the repository is private (always false for collected data) |
| `fork` | Boolean | Whether the repository is a fork of another repository |
| `default_branch` | String | Default branch name (usually 'main' or 'master') |
| `size` | Integer | Repository size in kilobytes |
| `stargazers_count` | Integer | Number of users who have starred the repository |
| `watchers_count` | Integer | Number of users watching the repository |
| `forks_count` | Integer | Number of repository forks |
| `open_issues_count` | Integer | Number of open issues |
| `created_at` | DateTime | Repository creation timestamp |
| `updated_at` | DateTime | Last update timestamp |
| `pushed_at` | DateTime | Last commit timestamp |

### Contributor Data

| Variable | Type | Description |
|----------|------|-------------|
| `id` | Integer | Unique GitHub user identifier |
| `login` | String | GitHub username |
| `name` | String | User's full name (if provided) |
| `email` | String | User's public email address (if provided) |
| `type` | String | Account type (usually 'User') |
| `avatar_url` | String | URL to user's profile image |
| `company` | String | Company affiliation (if provided) |
| `blog` | String | URL to user's blog or website |
| `location` | String | User-provided location string |
| `country_code` | String | ISO 2-letter country code (derived from location) |
| `region` | String | Region within country or continent (derived from location) |
| `bio` | Text | User's profile biography |
| `twitter_username` | String | User's Twitter/X username (if provided) |
| `public_repos` | Integer | Number of public repositories owned |
| `public_gists` | Integer | Number of public gists created |
| `followers` | Integer | Number of followers |
| `following` | Integer | Number of users being followed |
| `created_at` | DateTime | Account creation timestamp |
| `updated_at` | DateTime | Last profile update timestamp |

### Organization Data

| Variable | Type | Description |
|----------|------|-------------|
| `id` | Integer | Unique GitHub organization identifier |
| `login` | String | Organization username |
| `name` | String | Organization's full name |
| `email` | String | Organization's public email address |
| `type` | String | Account type (usually 'Organization') |
| `avatar_url` | String | URL to organization's profile image |
| `company` | String | Parent company (if applicable) |
| `blog` | String | URL to organization's website |
| `location` | String | Organization's location string |
| `country_code` | String | ISO 2-letter country code (derived from location) |
| `region` | String | Region within country or continent (derived from location) |
| `bio` | Text | Organization's profile description |
| `twitter_username` | String | Organization's Twitter/X username |
| `public_repos` | Integer | Number of public repositories |
| `public_gists` | Integer | Number of public gists |
| `followers` | Integer | Number of followers |
| `following` | Integer | Number of accounts being followed |
| `public_members` | Integer | Number of public members in the organization |
| `created_at` | DateTime | Organization creation timestamp |
| `updated_at` | DateTime | Last profile update timestamp |

### Relationship Data

#### Contributor-Repository Relationship

| Variable | Type | Description |
|----------|------|-------------|
| `contributor_id` | Integer | ID of the contributor |
| `repository_id` | Integer | ID of the repository |
| `contributions` | Integer | Number of contributions made by the user to the repository |
| `first_contribution_at` | DateTime | Timestamp of first contribution |
| `last_contribution_at` | DateTime | Timestamp of most recent contribution |

#### Contributor-Organization Relationship

| Variable | Type | Description |
|----------|------|-------------|
| `contributor_id` | Integer | ID of the contributor |
| `organization_id` | Integer | ID of the organization |
| `joined_at` | DateTime | Timestamp when the contributor joined the organization |

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```
# Database
DATABASE_URL=sqlite:///github_data.db

# GitHub API
GITHUB_API_TOKEN=your_github_token_here

# For multiple tokens, use comma-separated values
# GITHUB_API_TOKENS=token1,token2,token3

# BigQuery (optional)
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_MAX_BYTES=1000000000
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

### Command-Line Options

The main script supports various command-line options for customization:

```
# Get full help
python -m src.github_database.main --help
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.