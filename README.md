# GitHub Data Analytics Pipeline

Ein hochperformantes ETL-System zur Analyse von GitHub-Aktivitätsdaten durch Integration von GitHub Archive und GitHub API.

## 🌟 Features

- Effiziente Verarbeitung von GitHub Archive Events
- Anreicherung mit GitHub API-Daten
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
│       │   └── github_api.py       # API-Client und Rate-Limiting
│       │
│       ├── database/              # Datenbankmodelle und Verwaltung
│       │   ├── database.py        # SQLAlchemy-Modelle und Datenbankinitialisierung
│       │   └── migrations/        # Alembic Migrationsskripte
│       │
│       ├── enrichment/            # Datenanreicherung
│       │   └── data_enricher.py   # Anreicherung mit API-Daten
│       │
│       ├── github_archive/        # GitHub Archive Verarbeitung
│       │   └── github_archive.py  # Download und Parsing von Archivdaten
│       │
│       ├── mapping/              # Event-Mapping
│       │   └── repository_mapper.py # Mapping von Events zu Datenbankmodellen
│       │
│       ├── monitoring/           # Performance-Überwachung
│       │   └── performance_monitor.py # Metriken und Visualisierung
│       │
│       ├── processing/           # Datenverarbeitung
│       │   └── batch_processor.py # Effiziente Batch-Verarbeitung
│       │
│       ├── config.py            # Konfigurationsverwaltung
│       ├── etl_orchestrator.py  # ETL-Prozesssteuerung
│       └── main.py             # Hauptanwendung
│
├── tests/                      # Unittest-Suite
├── requirements.txt           # Python-Abhängigkeiten
└── README.md                 # Projektdokumentation
```

## 🔑 Hauptkomponenten

### ETL Orchestrator
- **Datei**: `etl_orchestrator.py`
- **Funktion**: Zentrale Steuerung des ETL-Prozesses
- **Features**:
  - Streaming-Verarbeitung von Archivdaten
  - Automatische Batch-Größenoptimierung
  - Fortschrittsverfolgung
  - Fehlerbehandlung und Wiederaufnahme

### Batch Processor
- **Datei**: `processing/batch_processor.py`
- **Funktion**: Effiziente Batch-Verarbeitung von Events
- **Features**:
  - Multi-Threading
  - Optimierte SQLAlchemy-Operationen
  - Event-Typ-basierte Queues
  - Automatische Ressourcenanpassung

### Data Enricher
- **Datei**: `enrichment/data_enricher.py`
- **Funktion**: Anreicherung von Daten mit GitHub API
- **Features**:
  - Mehrstufiges Caching (Memory + Disk)
  - Rate-Limiting-Verwaltung
  - Batch-Anreicherung
  - Fehlertoleranz

### Repository Mapper
- **Datei**: `mapping/repository_mapper.py`
- **Funktion**: Mapping von Events zu Datenbankmodellen
- **Features**:
  - Validierung von Event-Daten
  - Effiziente Objekterstellung
  - Caching häufig verwendeter Objekte
  - Thread-sichere Implementierung

### Performance Monitor
- **Datei**: `monitoring/performance_monitor.py`
- **Funktion**: Überwachung und Visualisierung der Performance
- **Features**:
  - Echtzeit-Metriken
  - Grafische Dashboards
  - Ressourcenüberwachung
  - Metrik-Persistenz

## 🚀 Verwendung

### Installation

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

### Konfiguration

```python
from github_database.config import ETLConfig

config = ETLConfig(
    api_token="your_github_token",
    database_url="sqlite:///github_data.db",
    batch_size=1000
)
```

### Datenverarbeitung starten

```python
from github_database.etl_orchestrator import ETLOrchestrator
from datetime import datetime, timedelta

# ETL-Prozess initialisieren
orchestrator = ETLOrchestrator(config)

# Zeitraum festlegen
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 7)

# Verarbeitung starten
orchestrator.process_archive(start_date, end_date)
```

## 📊 Performance-Optimierung

Das System enthält mehrere Optimierungen für hohe Performance:

1. **Batch-Verarbeitung**:
   - Automatische Batch-Größenanpassung
   - Effiziente Bulk-Operationen
   - Event-Typ-basiertes Batching

2. **Parallelisierung**:
   - Thread-Pool für gleichzeitige Verarbeitung
   - Thread-sichere Datenbankzugriffe
   - Optimierte Worker-Anzahl

3. **Caching**:
   - Mehrstufiges Caching-System
   - LRU-basierte Cache-Eviction
   - Persistenter Disk-Cache

4. **Datenbankoptimierung**:
   - Strategische Indizierung
   - Connection-Pooling
   - Effiziente Abfragemuster

5. **Speichermanagement**:
   - Streaming großer Dateien
   - Automatische Garbage-Collection
   - Speichereffiziente Datenstrukturen

## 📈 Monitoring

Das integrierte Monitoring-System bietet:

- Echtzeit-Performance-Metriken
- Grafische Dashboards
- CPU- und Speicherüberwachung
- Durchsatz- und Fehlerstatistiken

## 🛠 Tests

```bash
# Alle Tests ausführen
python -m pytest tests/

# Spezifische Test-Suite ausführen
python -m pytest tests/test_batch_processor.py
```

## 📝 Lizenz

Dieses Projekt ist unter der MIT-Lizenz lizenziert - siehe die [LICENSE](LICENSE) Datei für Details.