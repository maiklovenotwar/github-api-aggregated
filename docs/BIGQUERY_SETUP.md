# Google Cloud und BigQuery Setup Guide

Dieser Guide führt Sie durch die Einrichtung der Google Cloud-Umgebung für den Zugriff auf GitHub Archive Daten via BigQuery.

## 1. Google Cloud Projekt Einrichtung

1. Besuchen Sie die [Google Cloud Console](https://console.cloud.google.com)
2. Erstellen Sie ein neues Projekt:
   - Klicken Sie auf den Projekt-Dropdown oben in der Konsole
   - Wählen Sie "Neues Projekt"
   - Name: `github-api-archive`
   - Notieren Sie sich die Projekt-ID

## 2. BigQuery API Aktivierung

1. Gehen Sie zur [API-Bibliothek](https://console.cloud.google.com/apis/library)
2. Suchen Sie nach "BigQuery API"
3. Klicken Sie auf "Aktivieren"

## 3. Service Account und Credentials erstellen

1. Navigieren Sie zu [IAM & Admin > Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Klicken Sie auf "Service Account erstellen":
   - Name: `github-archive-reader`
   - Beschreibung: "Service Account für GitHub Archive Datenzugriff"
3. Vergeben Sie die Rollen:
   - BigQuery Data Viewer
   - BigQuery Job User
4. Erstellen Sie einen JSON-Schlüssel:
   - Klicken Sie auf den erstellten Service Account
   - Wählen Sie "Schlüssel" > "Neuen Schlüssel hinzufügen" > JSON
   - Speichern Sie die heruntergeladene JSON-Datei als:
     `/Users/maik/Desktop/Coden/github-api-archive/JSON Schlüssel/GitHub API Archive.json`

## 4. Projekt-Konfiguration

1. Erstellen Sie eine `.env` Datei basierend auf `.env.template`:
   ```bash
   cp .env.template .env
   ```

2. Konfigurieren Sie die folgenden Umgebungsvariablen in `.env`:
   ```
   BIGQUERY_PROJECT_ID=github-api-archive
   GOOGLE_APPLICATION_CREDENTIALS='/Users/maik/Desktop/Coden/github-api-archive/JSON Schlüssel/GitHub API Archive.json'
   BIGQUERY_MAX_BYTES=1000000000
   ```

3. Installieren Sie die erforderlichen Python-Pakete:
   ```bash
   pip install -r requirements.txt
   ```

## 5. Überprüfung der Installation

1. Testen Sie die BigQuery-Verbindung mit diesem Python-Snippet:
   ```python
   from google.cloud import bigquery
   
   # Initialisiere den Client
   client = bigquery.Client()
   
   # Teste eine einfache Abfrage
   query = """
   SELECT COUNT(*) as event_count
   FROM `githubarchive.day.20240321`
   LIMIT 1
   """
   
   try:
       results = client.query(query)
       for row in results:
           print(f"Anzahl der Events: {row.event_count}")
       print("BigQuery-Verbindung erfolgreich!")
   except Exception as e:
       print(f"Fehler: {e}")
   ```

## 6. Sicherheitshinweise

1. Behandeln Sie die Service Account JSON-Datei als sensibles Geheimnis:
   - Fügen Sie sie NICHT zur Versionskontrolle hinzu
   - Teilen Sie sie nicht öffentlich
   - Beschränken Sie den Dateizugriff auf Ihren Benutzer:
     ```bash
     chmod 600 "JSON Schlüssel/GitHub API Archive.json"
     ```

2. Die `.env` Datei ist bereits in `.gitignore` aufgeführt und wird nicht versioniert.

## 7. Kostenmanagement

1. Setzen Sie ein Budget-Alert in der Google Cloud Console:
   - Navigieren Sie zu [Billing > Budgets & Alerts](https://console.cloud.google.com/billing)
   - Erstellen Sie ein Budget mit monatlicher Warnung
   - Empfohlene Schwellen: 50%, 80%, 90% des Budgets

2. BigQuery-Kosten optimieren:
   - Nutzen Sie `BIGQUERY_MAX_BYTES` zur Kostenkontrolle
   - Verwenden Sie `SELECT COUNT(*)` vor großen Abfragen
   - Nutzen Sie Partitionierung und Clustering wo möglich

## 8. Nützliche Links

- [GitHub Archive in BigQuery](https://console.cloud.google.com/bigquery?p=githubarchive&d=day&page=dataset)
- [BigQuery Preisrechner](https://cloud.google.com/products/calculator)
- [GitHub Archive Schema](https://www.gharchive.org/#schema)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
