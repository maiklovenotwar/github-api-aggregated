from sqlalchemy import text
from src.github_database.database.database import engine

def cleanup_duplicates():
    """
    Entfernt Duplikate aus der Tabelle 'repositories', sodass pro repo_id nur der Datensatz mit dem
    kleinsten rowid erhalten bleibt.
    """
    # Die folgende SQL-Anweisung löscht alle Zeilen, deren rowid nicht der kleinsten rowid für eine gegebene repo_id entspricht.
    delete_query = text("""
        DELETE FROM repositories
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM repositories
            GROUP BY repo_id
        )
    """)
    
    with engine.connect() as conn:
        result = conn.execute(delete_query)
        # Bei SQLite gibt rowcount unter Umständen nicht immer die exakte Anzahl gelöschter Zeilen zurück.
        print(f"Bereinigung abgeschlossen. Geschätzt {result.rowcount} doppelte Einträge wurden entfernt.")

if __name__ == '__main__':
    cleanup_duplicates()