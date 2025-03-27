# GitHub Global Contribution Analysis

A high-performance ETL system for analyzing global GitHub contribution patterns through geographical data enrichment and analysis. This system focuses on efficient collection of GitHub repository data and geographical analysis of contribution patterns.

## 🌍 Project Focus

This project focuses on analyzing geographical patterns in open-source contributions by:

1. **Collecting comprehensive GitHub repository data** using efficient collection strategies
2. **Enriching data with geographical information** through geocoding of user and organization locations
3. **Analyzing contribution patterns across countries and regions** to identify global trends
4. **Providing insights into open-source participation by region** over time

## 🌟 Key Features

- **Efficient GitHub API Integration**: Optimized for collecting detailed repository and contributor metadata
- **Geographical Enrichment**: Extracts and geocodes location data from contributors and organizations
- **Efficient Collection Strategies**:
  - Star-based collection for popular repositories
  - Time-period collection for historical analysis with optimized period sizing
- **Performance Optimizations**:
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
- SQLAlchemy-compatible database (SQLite by default)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/github-api-aggregated.git
cd github-api-aggregated

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
# Collect repositories using time period strategy
python -m src.github_database.main --mode collect --collection-strategy time-period --start-year 2015 --end-year 2023 --min-stars 50 --limit 1000

# Enrich collected data with geographical information
python -m src.github_database.main --mode enrich

# Export data to CSV files
python -m src.github_database.main --mode export --output-dir ./exports
```

### Advanced Usage

```bash
# Parallel collection with multiple tokens
python -m src.github_database.main --mode collect --collection-strategy time-period --parallel --workers 10 --tokens TOKEN1 TOKEN2 TOKEN3 --start-year 2020 --end-year 2023 --min-stars 100 --limit 2000

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
│       │   ├── cache.py            # API response caching
│       │   ├── errors.py           # Error handling classes
│       │   └── simple_github_client.py # Simplified GitHub client
│       │
│       ├── database/               # Database models and management
│       │   └── database.py         # SQLAlchemy models (Repository, Contributor, Organization)
│       │
│       ├── geocoding/              # Geographical data enrichment
│       │   └── geocoder.py         # Location geocoding functionality
│       │
│       ├── config/                 # Configuration management
│       │   ├── config.py           # Configuration classes
│       │   ├── etl_config.py       # ETL configuration
│       │   └── github_config.py    # GitHub API configuration
│       │
│       ├── repository_collector.py # Repository collection with time-based strategy
│       └── main.py                 # Main application entry point
│
├── scripts/                        # Utility scripts
│   ├── collect_repositories.py     # Standalone repository collection script
│   ├── export_tables_to_csv.py     # Data export utilities
│   └── update_location_geocoding.py # Location geocoding script
│
├── .env.template                   # Environment variables template
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```

## 🔑 Core Components

### Repository Collector
- Implements a time-based collection strategy for GitHub repositories
- Breaks down collection into small time periods to work around API limits
- Tracks progress and supports resumable collection
- Optimized to capture repositories even when results exceed API limits

### GitHub API Client
- Manages authentication and rate limiting for GitHub API
- Implements token rotation for improved throughput
- Provides caching to reduce redundant API calls
- Handles error conditions and retries

### Database Models
- Implements SQLAlchemy models for Repository, Contributor, and Organization entities
- Manages relationships between entities
- Provides efficient query interfaces for data retrieval and analysis

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

### Database Schema Overview

The database follows a relational model with three primary entities and two relationship tables:

```
┌───────────────┐       ┌─────────────────────┐       ┌────────────────┐
│ Organization  │       │ contributor_repository │       │  Repository   │
├───────────────┤       ├─────────────────────┤       ├────────────────┤
│ id            │       │ contributor_id      │       │ id             │
│ login         │       │ repository_id       │       │ name           │
│ name          │◄──┐   │ contributions       │   ┌──►│ full_name      │
│ ...           │   │   │ first_contribution_at│   │   │ owner_id       │
└───────┬───────┘   │   │ last_contribution_at │   │   │ organization_id│
        │           │   └─────────────────────┘   │   │ ...            │
        │           │                             │   └────────┬───────┘
        │           │   ┌─────────────────────┐   │            │
        │           └───┤ contributor_organization ├───┘            │
        │               ├─────────────────────┤                │
        │               │ contributor_id      │                │
        │               │ organization_id     │                │
        │               │ joined_at           │                │
        │               └─────────────────────┘                │
        │                                                      │
        │               ┌────────────────┐                     │
        └───────────────┤  Contributor   ├─────────────────────┘
                        ├────────────────┤
                        │ id             │
                        │ login          │
                        │ name           │
                        │ ...            │
                        └────────────────┘
```

#### Key Relationships:

1. **Repository to Contributor** (Many-to-Many):
   - A repository can have multiple contributors
   - A contributor can contribute to multiple repositories
   - The `contributor_repository` table tracks this relationship with additional metadata about contributions

2. **Repository to Organization** (Many-to-One):
   - A repository can belong to one organization
   - An organization can have multiple repositories
   - The `organization_id` in the Repository table establishes this relationship

3. **Contributor to Organization** (Many-to-Many):
   - A contributor can be a member of multiple organizations
   - An organization can have multiple contributors
   - The `contributor_organization` table tracks this relationship

4. **Repository to Owner** (Many-to-One):
   - Every repository has exactly one owner (which is a Contributor)
   - A contributor can own multiple repositories
   - The `owner_id` in the Repository table establishes this relationship

### Repository Data

| Variable | Type | Description |
|----------|------|-------------|
| `id` | Integer | Unique GitHub repository identifier |
| `name` | String | Repository name (without owner) |
| `full_name` | String | Complete repository name in format `owner/repo` |
| `owner_id` | Integer | ID of the repository owner (contributor) - Foreign key to `contributors.id` |
| `organization_id` | Integer | ID of the organization if repository belongs to one - Foreign key to `organizations.id` |
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

#### Repository Data Usage:
- Repositories are the central entity in the database, representing GitHub code repositories
- Each repository is linked to its owner (a Contributor) and optionally to an Organization
- Repository metrics like stars, forks, and issues are used for popularity and activity analysis
- The language field enables programming language distribution analysis

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

#### Contributor Data Usage:
- Contributors represent GitHub users who contribute to repositories
- The `location`, `country_code`, and `region` fields are crucial for geographical analysis
- A contributor can be both a repository owner and a contributor to other repositories
- The same contributor data structure is used for individual users and organization owners

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

#### Organization Data Usage:
- Organizations represent GitHub organizations that host repositories
- Like contributors, organizations have geographical data (`location`, `country_code`, `region`)
- Organizations are linked to their repositories and contributors (members)
- Organization data enables analysis of organizational influence in open-source communities

### Relationship Data

#### Contributor-Repository Relationship

| Variable | Type | Description |
|----------|------|-------------|
| `contributor_id` | Integer | ID of the contributor - Foreign key to `contributors.id` |
| `repository_id` | Integer | ID of the repository - Foreign key to `repositories.id` |
| `contributions` | Integer | Number of contributions made by the user to the repository |
| `first_contribution_at` | DateTime | Timestamp of first contribution |
| `last_contribution_at` | DateTime | Timestamp of most recent contribution |

#### Contributor-Repository Relationship Usage:
- This junction table implements the many-to-many relationship between contributors and repositories
- It stores valuable metadata about the contribution relationship, such as:
  - The number of contributions made by each contributor to each repository
  - The timespan of contributions (first to last)
- This data enables temporal analysis of contribution patterns
- It's used to analyze how contributors engage with repositories over time

#### Contributor-Organization Relationship

| Variable | Type | Description |
|----------|------|-------------|
| `contributor_id` | Integer | ID of the contributor - Foreign key to `contributors.id` |
| `organization_id` | Integer | ID of the organization - Foreign key to `organizations.id` |
| `joined_at` | DateTime | Timestamp when the contributor joined the organization |

#### Contributor-Organization Relationship Usage:
- This junction table implements the many-to-many relationship between contributors and organizations
- It tracks which contributors are members of which organizations
- The `joined_at` timestamp allows for temporal analysis of organization membership
- This data is used to analyze organizational influence and contributor affiliations

### Data Collection and Enrichment Process

1. **Repository Collection**:
   - Repositories are collected using the GitHub API based on criteria like star count and creation date
   - The collection is performed in small time periods to work around API limitations
   - Basic repository metadata is stored in the `repositories` table

2. **Contributor Collection**:
   - For each repository, contributors are collected using the GitHub API
   - Contributor profiles are fetched to get detailed information
   - The contributor-repository relationship is established with contribution counts

3. **Organization Collection**:
   - Organizations are collected from repository ownership data
   - Organization profiles are fetched to get detailed information
   - Organization-contributor relationships are established

4. **Geographical Enrichment**:
   - Location strings from contributors and organizations are geocoded
   - Country codes and regions are extracted and stored
   - This enrichment enables geographical analysis of contributions

5. **Data Updates**:
   - The system can update existing data to reflect changes in repositories, contributors, and organizations
   - Updates preserve historical data while adding new information

### Data Analysis Capabilities

The database schema enables various types of analyses:

1. **Geographic Distribution Analysis**:
   - Map contributors and organizations by country and region
   - Analyze contribution patterns across different geographical areas
   - Identify emerging open-source hubs globally

2. **Temporal Analysis**:
   - Track contribution patterns over time
   - Analyze repository growth and activity
   - Identify trends in open-source participation

3. **Network Analysis**:
   - Analyze collaboration networks between contributors
   - Examine organizational relationships and influence
   - Identify key contributors and their impact across repositories

4. **Programming Language Analysis**:
   - Analyze the distribution of programming languages
   - Identify regional preferences for certain languages
   - Track language popularity trends over time

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