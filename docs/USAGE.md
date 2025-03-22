# Nutzungsanleitung: GitHub Data Analytics Pipeline

Diese Anleitung beschreibt die verschiedenen Anwendungsfälle und Konfigurationsmöglichkeiten der GitHub Data Analytics Pipeline.

## 📋 Inhaltsverzeichnis

1. [Grundlegende Nutzung](#grundlegende-nutzung)
2. [Anwendungsfälle](#anwendungsfälle)
3. [Konfigurationsbeispiele](#konfigurationsbeispiele)
4. [Optimierung](#optimierung)
5. [Fehlerbehebung](#fehlerbehebung)

## 🚀 Grundlegende Nutzung

### Installation und Setup

1. **Repository klonen und Abhängigkeiten installieren**:
```bash
git clone https://github.com/yourusername/github-api.git
cd github-api
python -m venv venv
source venv/bin/activate  # Unix
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

2. **Umgebungsvariablen konfigurieren**:
```bash
cp .env.template .env
```

Bearbeiten Sie `.env`:
```plaintext
# GitHub API Konfiguration
GITHUB_API_TOKEN=your_token_here

# BigQuery Konfiguration
BIGQUERY_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
BIGQUERY_MAX_BYTES=1000000000

# Datenbank Konfiguration
DATABASE_URL=sqlite:///github_data.db
```

### Basis-Skript

```python
from github_database.config import ETLConfig
from github_database.config.bigquery_config import BigQueryConfig
from github_database.etl_orchestrator import ETLOrchestrator
from datetime import datetime, timedelta

# Konfiguration
config = ETLConfig.from_env()

# ETL-Prozess initialisieren
orchestrator = ETLOrchestrator(config)

# Zeitraum definieren
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# Verarbeitung starten
orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    min_stars=50,
    min_forks=10
)
```

## 💡 Anwendungsfälle

### 1. Repository-Analyse

#### Einzelnes Repository analysieren
```python
# Repository by ID
orchestrator.process_repositories(
    repo_ids=[12345678],
    start_date=start_date,
    end_date=end_date
)

# Repository by Name
orchestrator.process_repositories(
    repo_names=["owner/repo"],
    start_date=start_date,
    end_date=end_date
)
```

#### Top Repositories nach Sternen
```python
orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    min_stars=1000,
    limit=100
)
```

### 2. Event-Analyse

#### Spezifische Event-Typen
```python
orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    event_types=["PushEvent", "PullRequestEvent"],
    min_stars=50
)
```

#### Aktivitätsanalyse
```python
orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    min_commits_last_year=100,
    min_contributors=10
)
```

### 3. Zeitreihenanalyse

#### Stündliche Granularität
```python
from datetime import timedelta

# Analyse der letzten 24 Stunden
end_date = datetime.now()
start_date = end_date - timedelta(hours=24)

orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    time_resolution="hour",
    min_stars=100
)
```

#### Historische Analyse
```python
# Analyse eines ganzen Jahres
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    time_resolution="day",
    batch_size=30  # Tage pro Batch
)
```

## ⚙️ Konfigurationsbeispiele

### 1. Minimale Konfiguration
```python
config = ETLConfig(
    api_token="your_token",
    database_url="sqlite:///github_data.db",
    bigquery_config=BigQueryConfig(
        project_id="your-project"
    )
)
```

### 2. Performance-Optimierte Konfiguration
```python
config = ETLConfig(
    api_token="your_token",
    database_url="sqlite:///github_data.db",
    bigquery_config=BigQueryConfig(
        project_id="your-project",
        max_bytes=5000000000,  # 5GB
        timeout=300  # 5 Minuten
    ),
    batch_size=1000,
    max_workers=4,
    cache_size=1000
)
```

### 3. Kostenoptimierte Konfiguration
```python
config = ETLConfig(
    api_token="your_token",
    database_url="sqlite:///github_data.db",
    bigquery_config=BigQueryConfig(
        project_id="your-project",
        max_bytes=1000000000,  # 1GB
        dry_run=True  # Kostenabschätzung
    ),
    cache_ttl=3600,  # 1 Stunde
    rate_limit_buffer=0.1  # 10% Reserve
)
```

## 🔧 Optimierung

### 1. Memory-Optimierung

```python
# Große Datensätze
config = ETLConfig(
    batch_size=100,  # Kleinere Batches
    stream_results=True,  # Streaming-Modus
    cache_size=500  # Reduzierter Cache
)
```

### 2. Performance-Optimierung

```python
# Maximale Performance
config = ETLConfig(
    max_workers=8,  # Mehr Worker
    batch_size=2000,  # Größere Batches
    cache_size=5000,  # Größerer Cache
    bigquery_config=BigQueryConfig(
        max_bytes=10000000000  # 10GB
    )
)
```

### 3. Kosten-Optimierung

```python
# Kosteneffizient
config = ETLConfig(
    rate_limit_buffer=0.2,  # 20% API-Reserve
    cache_ttl=7200,  # 2 Stunden Cache
    bigquery_config=BigQueryConfig(
        max_bytes=1000000000,  # 1GB
        dry_run=True  # Kostencheck
    )
)
```

## 🔍 Fehlerbehebung

### 1. API-Fehler

#### Rate Limiting
```python
# Rate Limit überschritten
orchestrator.process_repositories(
    rate_limit_buffer=0.3,  # 30% Reserve
    retry_count=3,
    retry_delay=60  # 1 Minute Wartezeit
)
```

#### Timeout-Fehler
```python
# Timeout-Behandlung
config = ETLConfig(
    api_timeout=30,  # 30 Sekunden
    bigquery_config=BigQueryConfig(
        timeout=300  # 5 Minuten
    )
)
```

### 2. BigQuery-Fehler

#### Quota-Überschreitung
```python
# Quota-Management
config = ETLConfig(
    bigquery_config=BigQueryConfig(
        max_bytes=1000000000,  # 1GB Limit
        location="EU",  # Spezifische Region
        dry_run=True  # Kostencheck
    )
)
```

#### Query-Timeout
```python
# Timeout-Behandlung
orchestrator.process_repositories(
    batch_size=100,  # Kleinere Batches
    retry_count=3,
    retry_delay=30
)
```

### 3. Speicher-Fehler

#### OOM-Fehler
```python
# Out of Memory Behandlung
config = ETLConfig(
    batch_size=50,  # Sehr kleine Batches
    stream_results=True,
    cache_size=100,
    gc_frequency=1000  # Häufigere GC
)
```

### 4. Debug-Modus

```python
# Debugging aktivieren
config = ETLConfig(
    debug=True,
    log_level="DEBUG",
    profile=True  # Performance-Profiling
)
```

## 📊 Monitoring

### Performance-Überwachung
```python
# Monitoring aktivieren
orchestrator.enable_monitoring(
    metrics=["cpu", "memory", "api_calls", "query_cost"],
    interval=60  # 1 Minute
)
```

### Kostenüberwachung
```python
# Kosten-Tracking
orchestrator.enable_cost_monitoring(
    budget_limit=10.0,  # 10 USD
    alert_threshold=0.8  # 80% des Budgets
)
```
