from ..database.database import get_session, Repository, Contributor, Organization, Commit, PullRequest, Issue, Branch, Fork, Star
import os

def get_db_size():
    """Gibt die Größe der Datenbank in MB zurück"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database.db')  # Korrigierter Pfad
    size_bytes = os.path.getsize(db_path)
    return size_bytes / (1024 * 1024)  # Konvertiere zu MB

def show_table_sizes(session):
    """Zeigt die Größe jeder Tabelle basierend auf den Spaltentypen an"""
    tables = {
        'repositories': Repository,
        'contributors': Contributor,
        'organizations': Organization,
        'commits': Commit,
        'pull_requests': PullRequest,
        'issues': Issue,
        'branches': Branch,
        'forks': Fork,
        'stars': Star
    }
    
    print("\nGeschätzte Datenmengen pro Tabelle:")
    for table_name, model in tables.items():
        # Zähle die Anzahl der Datensätze
        count = session.query(model).count()
        
        # Schätze die durchschnittliche Zeilengröße basierend auf den Spaltentypen
        avg_row_size = 0
        for column in model.__table__.columns:
            if str(column.type) in ['INTEGER', 'BOOLEAN']:
                avg_row_size += 4
            elif str(column.type) == 'DATETIME':
                avg_row_size += 8
            elif str(column.type) in ['VARCHAR', 'TEXT']:
                avg_row_size += 100  # Geschätzte durchschnittliche Länge
        
        # Berechne die geschätzte Gesamtgröße
        total_size = (count * avg_row_size) / (1024 * 1024)  # In MB
        print(f"  {table_name:.<20} {count:>6} Einträge, ca. {total_size:.2f} MB")

def show_overview():
    session = get_session()

    # Anzahl der Datensätze in den einzelnen Tabellen
    repo_count = session.query(Repository).count()
    contrib_count = session.query(Contributor).count()
    org_count = session.query(Organization).count()
    commit_count = session.query(Commit).count()
    pr_count = session.query(PullRequest).count()
    issue_count = session.query(Issue).count()
    branch_count = session.query(Branch).count()
    fork_count = session.query(Fork).count()
    star_count = session.query(Star).count()

    total_records = repo_count + contrib_count + org_count + commit_count + pr_count + issue_count + branch_count + fork_count + star_count
    db_size = get_db_size()

    print("\n=== Datenbank-Übersicht ===")
    print(f"Gesamtgröße der Datenbank: {db_size:.2f} MB")
    print(f"Gesamtanzahl Datensätze: {total_records:,}")
    
    print("\nAnzahl Datensätze pro Tabelle:")
    print("┌──────────────────┬────────┐")
    print("│ Tabelle         │ Anzahl │")
    print("├──────────────────┼────────┤")
    print(f"│ Repositories    │ {repo_count:>6} │")
    print(f"│ Contributors    │ {contrib_count:>6} │")
    print(f"│ Organizations   │ {org_count:>6} │")
    print(f"│ Commits         │ {commit_count:>6} │")
    print(f"│ Pull Requests   │ {pr_count:>6} │")
    print(f"│ Issues          │ {issue_count:>6} │")
    print(f"│ Branches        │ {branch_count:>6} │")
    print(f"│ Forks           │ {fork_count:>6} │")
    print(f"│ Stars           │ {star_count:>6} │")
    print("└──────────────────┴────────┘")

    # Zeige die geschätzten Datenmengen pro Tabelle
    show_table_sizes(session)

    print("\nBeispieldatensätze (max. 5 pro Tabelle):")
    print("\nRepositories:")
    for repo in session.query(Repository).limit(5).all():
        print(f"  ID: {repo.repo_id} | Name: {repo.name} | Created At: {repo.created_at} | Language: {repo.language}")

    print("\nContributor:")
    for contrib in session.query(Contributor).limit(5).all():
        print(f"  User ID: {contrib.user_id} | Username: {contrib.username} | Contributions: {contrib.contributions}")

    session.close()

if __name__ == "__main__":
    show_overview()