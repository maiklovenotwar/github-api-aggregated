from src.github_database.database.database import get_session, Repository

def validate_repositories():
    session = get_session()
    # Zähle die Anzahl der Datensätze in der Tabelle "repositories"
    count = session.query(Repository).count()
    print("Anzahl der Repositories in der Datenbank:", count)
    
    # Zeige die ersten 5 Einträge an, um einen Überblick über die Daten zu bekommen
    repos = session.query(Repository).limit(1000).all()
    for repo in repos:
        print(f"Repo ID: {repo.repo_id}, Name: {repo.name}, Owner: {repo.owner}, Sprache: {repo.language}")

def main():
    validate_repositories()

if __name__ == '__main__':
    main()