#!/usr/bin/env python3
"""
Exportiert alle Tabellen aus der GitHub-Datenbank als CSV-Dateien.
"""

import os
import sys
import logging
import pandas as pd
import sqlite3
from pathlib import Path

# Füge das Projekt-Verzeichnis zum Python-Pfad hinzu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Konfiguriere Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def export_table_to_csv(db_path, table_name, output_dir="exports"):
    """
    Exportiert eine Tabelle aus der Datenbank als CSV-Datei.
    
    Args:
        db_path: Pfad zur SQLite-Datenbank
        table_name: Name der zu exportierenden Tabelle
        output_dir: Ausgabeverzeichnis für die CSV-Dateien
    
    Returns:
        Pfad zur exportierten CSV-Datei
    """
    # Ausgabeverzeichnis erstellen, falls es nicht existiert
    os.makedirs(output_dir, exist_ok=True)
    
    # Verbindung zur Datenbank herstellen
    conn = sqlite3.connect(db_path)
    
    # Tabelle in DataFrame laden
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql_query(query, conn)
        
        # Anzahl der Zeilen in der Tabelle
        row_count = len(df)
        
        # CSV-Datei speichern
        output_file = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(output_file, index=False)
        
        logger.info(f"Tabelle '{table_name}' mit {row_count} Zeilen exportiert nach '{output_file}'")
        return output_file
    except Exception as e:
        logger.error(f"Fehler beim Exportieren der Tabelle '{table_name}': {e}")
        return None
    finally:
        conn.close()

def get_all_tables(db_path):
    """
    Gibt eine Liste aller Tabellen in der Datenbank zurück.
    
    Args:
        db_path: Pfad zur SQLite-Datenbank
    
    Returns:
        Liste der Tabellennamen
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Alle Tabellen abfragen
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tables

def main():
    """Hauptfunktion zum Exportieren aller Tabellen."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Exportiert alle Tabellen aus der GitHub-Datenbank als CSV-Dateien.")
    parser.add_argument("--db-path", default="github_data.db", help="Pfad zur SQLite-Datenbank")
    parser.add_argument("--output-dir", default="exports", help="Ausgabeverzeichnis für die CSV-Dateien")
    parser.add_argument("--tables", nargs="+", help="Spezifische Tabellen zum Exportieren (optional)")
    
    args = parser.parse_args()
    
    logger.info(f"Exportiere Tabellen aus der Datenbank '{args.db_path}'")
    
    # Alle Tabellen in der Datenbank abrufen
    if args.tables:
        tables = args.tables
        logger.info(f"Exportiere spezifische Tabellen: {', '.join(tables)}")
    else:
        tables = get_all_tables(args.db_path)
        logger.info(f"Gefundene Tabellen: {', '.join(tables)}")
    
    # Jede Tabelle exportieren
    exported_files = []
    for table in tables:
        # Interne SQLite-Tabellen überspringen
        if table == "sqlite_sequence":
            logger.info(f"Überspringe interne SQLite-Tabelle: {table}")
            continue
            
        output_file = export_table_to_csv(args.db_path, table, args.output_dir)
        if output_file:
            exported_files.append(output_file)
    
    logger.info(f"Export abgeschlossen. {len(exported_files)} Tabellen wurden exportiert.")
    
    # Zusammenfassung anzeigen
    for file in exported_files:
        file_size = os.path.getsize(file) / 1024  # KB
        logger.info(f"  - {os.path.basename(file)}: {file_size:.2f} KB")

if __name__ == "__main__":
    main()
