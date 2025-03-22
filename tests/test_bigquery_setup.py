"""Test BigQuery setup and configuration."""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

def test_bigquery_connection():
    """Test BigQuery connection and basic query functionality."""
    
    # Load environment variables
    load_dotenv()
    
    # Check required environment variables
    required_vars = ['BIGQUERY_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Fehlende Umgebungsvariablen: {', '.join(missing_vars)}")
        return False
        
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        
        # Test query - get event count for a specific day
        test_date = datetime.now() - timedelta(days=7)  # Eine Woche zurück
        table_date = test_date.strftime('%Y%m%d')
        
        query = f"""
        SELECT COUNT(*) as event_count
        FROM `githubarchive.day.{table_date}`
        LIMIT 1
        """
        
        # Execute query
        print(f"🔄 Führe Test-Query für Datum {table_date} aus...")
        query_job = client.query(query)
        results = query_job.result()
        
        # Get results
        for row in results:
            print(f"✅ Erfolgreich! Gefundene Events: {row.event_count:,}")
            
        return True
        
    except Exception as e:
        print(f"❌ Fehler beim Testen der BigQuery-Verbindung: {str(e)}")
        return False

if __name__ == "__main__":
    print("🔍 Teste BigQuery-Setup...")
    success = test_bigquery_connection()
    
    if not success:
        print("\n❌ BigQuery-Setup fehlgeschlagen. Bitte überprüfen Sie die Dokumentation in docs/BIGQUERY_SETUP.md")
