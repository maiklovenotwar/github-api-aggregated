from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import datetime
import os

# Basis-Klasse für ORM-Modelle
Base = declarative_base()

# =============================================
# Tabelle: repositories
# =============================================
class Repository(Base):
    __tablename__ = 'repositories'
    
    id = Column(Integer, primary_key=True)  # Interne ID
    repo_id = Column(Integer, unique=True, nullable=False)  # GitHub-spezifische ID
    name = Column(Text, nullable=False)  # Name des Repositories
    owner = Column(Integer, nullable=False)  # Referenz zu einem Contributor oder einer Organization (wird als integer gespeichert)
    owner_login = Column(String, nullable=False)  # GitHub Login/Username des Repository-Owners
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    description = Column(Text)
    language = Column(Text)
    default_branch = Column(Text)  # Standard-Branch (z. B. "main")
    forks_count = Column(Integer)
    stars_count = Column(Integer)
    open_issues_count = Column(Integer)
    
    # Optionale Beziehungen (falls gewünscht, können diese später verfeinert werden)
    commits = relationship("Commit", back_populates="repository")
    pull_requests = relationship("PullRequest", back_populates="repository")
    issues = relationship("Issue", back_populates="repository")
    branches = relationship("Branch", back_populates="repository")
    forks = relationship("Fork", back_populates="repository")
    stars = relationship("Star", back_populates="repository")


# =============================================
# Tabelle: contributors
# =============================================
class Contributor(Base):
    __tablename__ = 'contributors'
    
    id = Column(Integer, primary_key=True)  # Interne ID
    user_id = Column(Integer, unique=True, nullable=False)  # GitHub-User-ID
    username = Column(String, nullable=False)  # GitHub-Username
    email = Column(String)  # E-Mail-Adresse (falls öffentlich)
    company = Column(Text)
    location = Column(Text)
    contributions = Column(Integer, default=0)  # Anzahl der Commits/PRs/Issues (später aktualisierbar)
    created_at = Column(DateTime)
    
    # Beziehungen zu Commits, PRs, Issues etc.
    commits = relationship("Commit", back_populates="author")
    pull_requests = relationship("PullRequest", back_populates="author")
    issues = relationship("Issue", back_populates="author")
    stars = relationship("Star", back_populates="starred_by_contributor")
    forks = relationship("Fork", back_populates="forked_by_contributor")


# =============================================
# Tabelle: organizations
# =============================================
class Organization(Base):
    __tablename__ = 'organizations'
    
    id = Column(Integer, primary_key=True)  # Interne ID
    org_id = Column(Integer, unique=True, nullable=False)  # GitHub Org-ID
    name = Column(Text, nullable=False)  # Name der Organisation (häufig in "login" enthalten)
    website = Column(Text)  # Website, z. B. aus "blog"
    email = Column(Text)
    location = Column(Text)
    created_at = Column(DateTime)


# =============================================
# Tabelle: forks
# =============================================
class Fork(Base):
    __tablename__ = 'forks'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)  # Referenz auf das geforkte Repository
    forked_by = Column(Integer, ForeignKey('contributors.id'), nullable=False)  # Contributor-ID des Forkers
    created_at = Column(DateTime)
    
    repository = relationship("Repository", back_populates="forks")
    forked_by_contributor = relationship("Contributor", back_populates="forks")


# =============================================
# Tabelle: stars
# =============================================
class Star(Base):
    __tablename__ = 'stars'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)  # Referenz auf das Repository
    starred_by = Column(Integer, ForeignKey('contributors.id'), nullable=False)  # Contributor-ID desjenigen, der gestarred hat
    starred_at = Column(DateTime)
    
    repository = relationship("Repository", back_populates="stars")
    starred_by_contributor = relationship("Contributor", back_populates="stars")


# =============================================
# Tabelle: commits
# =============================================
class Commit(Base):
    __tablename__ = 'commits'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)  # Zu welchem Repository gehört der Commit?
    commit_hash = Column(Text, unique=True, nullable=False)  # GitHub Commit-Hash (SHA)
    author_id = Column(Integer, ForeignKey('contributors.id'))  # Contributor, der den Commit gemacht hat
    message = Column(Text)
    committed_at = Column(DateTime)
    
    repository = relationship("Repository", back_populates="commits")
    author = relationship("Contributor", back_populates="commits")


# =============================================
# Tabelle: pull_requests
# =============================================
class PullRequest(Base):
    __tablename__ = 'pull_requests'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)  # Zu welchem Repository gehört der PR?
    github_id = Column(Integer, unique=True, nullable=False)  # GitHub PR-ID (Achtung: API liefert oft "number" und "id")
    title = Column(Text)
    state = Column(Text)  # z. B. "open", "closed", "merged"
    created_at = Column(DateTime)
    merged_at = Column(DateTime, nullable=True)  # Null, falls nicht gemerged
    author_id = Column(Integer, ForeignKey('contributors.id'))  # Wer hat den PR erstellt?
    
    repository = relationship("Repository", back_populates="pull_requests")
    author = relationship("Contributor", back_populates="pull_requests")


# =============================================
# Tabelle: issues
# =============================================
class Issue(Base):
    __tablename__ = 'issues'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    github_id = Column(Integer, unique=True, nullable=False)  # GitHub Issue-ID
    title = Column(Text)
    state = Column(Text)  # "open" oder "closed"
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    author_id = Column(Integer, ForeignKey('contributors.id'))  # Wer hat das Issue erstellt?
    
    repository = relationship("Repository", back_populates="issues")
    author = relationship("Contributor", back_populates="issues")


# =============================================
# Tabelle: branches
# =============================================
class Branch(Base):
    __tablename__ = 'branches'
    
    id = Column(Integer, primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    name = Column(Text)  # Name des Branches
    is_default = Column(Boolean)  # Ist der Branch der Standard-Branch?
    created_at = Column(DateTime)  # Hinweis: GitHub liefert standardmäßig nicht den Erstellungszeitpunkt eines Branches.
    
    repository = relationship("Repository", back_populates="branches")

# =============================================
# Engine und Session einrichten
# =============================================
DB_PATH = os.path.join(os.path.dirname(__file__), 'database.db')  # Datenbank im selben Ordner wie die Models
engine = create_engine(f'sqlite:///{DB_PATH}', echo=True)
Session = sessionmaker(bind=engine)

def init_db():
    """
    Initialisiert die Datenbank und erstellt alle Tabellen.
    """
    Base.metadata.create_all(engine)

def get_session():
    """
    Gibt eine Session zurück, über die Datenbankoperationen ausgeführt werden können.
    """
    return Session()

# Initialisiere die Datenbank beim Import
init_db()

# =============================================
# Beispielhafte Hilfsfunktion: Erstelle Repository aus API-Daten
# =============================================
def create_repository_from_api(api_data):
    """
    Wandelt API-Daten in ein Repository-Objekt um.
    Erwartet ein dict (JSON) mit den Repositorien-Daten von GitHub.
    """
    repo = Repository(
        repo_id=api_data.get("id"),
        name=api_data.get("name"),
        owner=api_data.get("owner", {}).get("id"),  # GitHub-ID des Owners
        owner_login=api_data.get("owner", {}).get("login"),  # GitHub Login/Username des Owners
        description=api_data.get("description"),
        created_at=datetime.datetime.strptime(api_data.get("created_at"), '%Y-%m-%dT%H:%M:%SZ') if api_data.get("created_at") else None,
        updated_at=datetime.datetime.strptime(api_data.get("updated_at"), '%Y-%m-%dT%H:%M:%SZ') if api_data.get("updated_at") else None,
        language=api_data.get("language"),
        default_branch=api_data.get("default_branch"),
        forks_count=api_data.get("forks_count"),
        stars_count=api_data.get("stargazers_count"),  # API liefert "stargazers_count"
        open_issues_count=api_data.get("open_issues_count")
    )
    return repo

def create_contributor_from_api(api_data):
    """
    Wandelt API-Daten eines Contributors in ein Contributor-Objekt um.
    Erwartet ein dict mit den Contributor-Daten, typischerweise aus dem /repos/:owner/:repo/contributors-Endpunkt.
    """
    contributor = Contributor(
        user_id = api_data.get("id"),
        username = api_data.get("login"),
        contributions = api_data.get("contributions"),
        # Die Felder email, company, location und created_at sind oft nicht in dieser API-Antwort enthalten.
        # Um sie zu erhalten, könntest du einen zusätzlichen API-Aufruf für den User (GET /users/{username}) machen.
        email = None,
        company = None,
        location = None,
        created_at = None
    )
    return contributor