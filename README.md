# GitHub Data Analytics Pipeline

Ein hochperformantes ETL-System zur Analyse von GitHub-Aktivitätsdaten durch hybride Integration von GitHub API und BigQuery.

## 🌟 Features

- Hybride Datenerfassung (GitHub API + BigQuery)
- Effiziente Verarbeitung von GitHub Archive Events via BigQuery
- Anreicherung mit GitHub API-Metadaten
- Parallele Batch-Verarbeitung
- Intelligentes Caching-System
- Performance-Monitoring und Visualisierung
- Robuste Fehlerbehandlung und Wiederaufnahme

## 🏗 Projektstruktur

```
github-api/
├── src/
│   └── github_database/
│       ├── api/                    # GitHub API Integration
│       │   ├── github_api.py       # API-Client und Rate-Limiting
│       │   └── bigquery_api.py     # BigQuery-Client für GitHub Archive
│       │
│       ├── analysis/               # Datenanalyse-Komponenten
│       │   ├── location_analysis.py # Standortbasierte Analyse
│       │   └── organization_analysis.py # Organisationsanalyse
│       │
│       ├── control_database/       # Datenbankvalidierung und -kontrolle
│       │   └── validate_data.py    # Datenvalidierungsfunktionen
│       │
│       ├── database/               # Datenbankmodelle und Verwaltung
│       │   ├── database.py         # SQLAlchemy-Modelle
│       │   └── migrations/         # Alembic Migrationsskripte
│       │
│       ├── enrichment/             # Datenanreicherung
│       │   └── data_enricher.py    # Anreicherung mit API-Daten
│       │
│       ├── config/                 # Konfiguration
│       │   ├── config.py           # Hauptkonfiguration
│       │   └── bigquery_config.py  # BigQuery-spezifische Konfiguration
│       │
│       ├── monitoring/             # Performance-Überwachung
│       │   └── performance_monitor.py # Metriken und Visualisierung
│       │
│       ├── github_archive.py       # GitHub Archive Event-Typen
│       ├── etl_orchestrator.py     # Hybride ETL-Prozesssteuerung
│       └── main.py                 # Hauptanwendung
│
├── docs/                           # Dokumentation
│   ├── ARCHITECTURE.md             # Systemarchitektur
│   ├── BIGQUERY_SETUP.md           # BigQuery-Einrichtung
│   └── USAGE.md                    # Nutzungsanleitungen
│
├── tests/                          # Testsuite
│   ├── test_hybrid_pipeline.py     # Tests für hybride Pipeline
│   └── analyze_test_results.py     # Testanalyse und Visualisierung
│
├── .env.template                  # Umgebungsvariablen-Template
├── requirements.txt               # Python-Abhängigkeiten
└── README.md                      # Projektdokumentation
```

## 🔑 Hauptkomponenten

### Hybride ETL-Pipeline
- **Datei**: `etl_orchestrator.py`
- **Funktion**: Orchestrierung der hybriden Datenerfassung
- **Features**:
  - Parallele Verarbeitung von API- und BigQuery-Daten
  - Intelligente Lastverteilung
  - Automatische Fehlerbehandlung
  - Fortschrittsverfolgung

### BigQuery Integration
- **Datei**: `bigquery/bigquery_client.py`
- **Funktion**: Effiziente Abfrage historischer GitHub-Daten
- **Features**:
  - Optimierte SQL-Queries
  - Kostenkontrolle
  - Streaming-Verarbeitung
  - Automatische Retry-Logik

### GitHub API Client
- **Datei**: `api/github_api.py`
- **Funktion**: Metadaten-Erfassung und Anreicherung
- **Features**:
  - Rate-Limiting-Management
  - Caching-System
  - Parallele Anfragen
  - Fehlertoleranz

## 🚀 Schnellstart

### 1. Installation

```bash
# Repository klonen
git clone https://github.com/yourusername/github-api.git
cd github-api

# Virtuelle Umgebung erstellen und aktivieren
python -m venv venv
source venv/bin/activate  # Unix
venv\Scripts\activate     # Windows

# Abhängigkeiten installieren
pip install -r requirements.txt
```

### 2. Google Cloud Setup

Folgen Sie der Anleitung in `docs/BIGQUERY_SETUP.md` für:
- Google Cloud Projekt-Einrichtung
- Service Account-Erstellung
- BigQuery API-Aktivierung
- Credentials-Konfiguration

### 3. Konfiguration

1. Umgebungsvariablen einrichten:
```bash
cp .env.template .env
# .env bearbeiten und Werte einfügen
```

2. Python-Konfiguration:
```python
from github_database.config import ETLConfig
from github_database.config.bigquery_config import BigQueryConfig

# BigQuery-Konfiguration
bigquery_config = BigQueryConfig(
    project_id="your-project-id",
    dataset_id="githubarchive",
    credentials_path="/path/to/credentials.json"
)

# ETL-Konfiguration
config = ETLConfig(
    api_token="your_github_token",
    database_url="sqlite:///github_data.db",
    bigquery_config=bigquery_config
)
```

### 4. Datenverarbeitung starten

```python
from github_database.etl_orchestrator import ETLOrchestrator
from datetime import datetime, timedelta

# ETL-Prozess initialisieren
orchestrator = ETLOrchestrator(config)

# Zeitraum festlegen
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 7)

# Verarbeitung starten
orchestrator.process_repositories(
    start_date=start_date,
    end_date=end_date,
    min_stars=50,  # Qualitätsfilter
    min_forks=10
)
```

## 💡 Vorteile des hybriden Ansatzes

1. **Effiziente Datenerfassung**:
   - Historische Daten via BigQuery
   - Aktuelle Metadaten via GitHub API
   - Optimale Ressourcennutzung

2. **Kostenoptimierung**:
   - Reduzierte API-Aufrufe
   - Effiziente BigQuery-Nutzung
   - Intelligentes Caching

3. **Verbesserte Datenqualität**:
   - Kreuzvalidierung von Datenquellen
   - Umfassendere Datensätze
   - Aktuelle Metadaten

4. **Hohe Performance**:
   - Parallele Verarbeitung
   - Optimierte Queries
   - Effizientes Ressourcenmanagement

## 📊 Performance-Monitoring

Das integrierte Monitoring-System bietet:

- Echtzeit-Performance-Metriken
- BigQuery-Kostenüberwachung
- API-Rate-Limiting-Statistiken
- Ressourcenauslastung

## 📋 Status der Module

### Aktiv verwendete Kernmodule
- **main.py**: Haupteinstiegspunkt der Anwendung
- **etl_orchestrator.py**: Kernkomponente für die ETL-Prozesse
- **api/github_api.py** und **api/bigquery_api.py**: API-Clients für GitHub und BigQuery
- **database/database.py**: Datenbankmodelle und Initialisierung
- **config/**: Konfigurationsmodule für die Anwendung

### Module für zukünftige Erweiterungen
Folgende Module sind für zukünftige Funktionalitäten vorgesehen und werden derzeit nicht aktiv in der Hauptanwendung verwendet:

1. **Analyse-Module** (`analysis/`):
   - `location_analysis.py`: Standortbasierte Analyse von Repositories und Nutzern
   - `organization_analysis.py`: Analyse von Organisationsaktivitäten
   - `visualization.py`: Visualisierungskomponenten für Analyseergebnisse

2. **Datenbank-Kontrollmodule** (`control_database/`):
   - `cleanup_duplicates.py`: Bereinigung von Duplikaten in der Datenbank
   - `control_data.py`: Kontrollfunktionen für Datenbankoperationen
   - `validate_data.py`: Validierung von Daten vor dem Import

3. **Datenanreicherung und Mapping** (`enrichment/`, `mapping/`):
   - `data_enricher.py`: Anreicherung von GitHub-Daten mit zusätzlichen Informationen
   - `repository_mapper.py`: Mapping von GitHub-Archive-Events auf Datenbankmodelle

4. **Monitoring und Batch-Verarbeitung** (`monitoring/`, `processing/`):
   - `performance_monitor.py`: Überwachung der Anwendungsleistung
   - `batch_processor.py`: Effiziente Batch-Verarbeitung für Datenbankoperationen

Diese Module bieten eine solide Grundlage für zukünftige Erweiterungen des Systems und können je nach Bedarf aktiviert und in die Hauptanwendung integriert werden.

## 🧪 Tests

```bash
# Hybrid Pipeline testen
python -m tests.test_hybrid_pipeline

# Testergebnisse analysieren
python -m tests.analyze_test_results
```

## 📚 Weiterführende Dokumentation

- [Systemarchitektur](docs/ARCHITECTURE.md)
- [BigQuery Setup](docs/BIGQUERY_SETUP.md)
- [Nutzungsanleitungen](docs/USAGE.md)

## 📝 Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert - siehe die [LICENSE](LICENSE) Datei für Details.