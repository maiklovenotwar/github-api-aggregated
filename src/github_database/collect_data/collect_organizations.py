import requests
import os
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Organization, Repository
import os

# Lade die Umgebungsvariablen
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_organization_details(login):
    """
    Ruft Details einer Organisation vom /orgs/{login}-Endpunkt ab.
    """
    url = f"https://api.github.com/orgs/{login}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der Organisation {login}: HTTP {response.status_code}")
        return None

def create_organization_from_api(api_data):
    """
    Wandelt API-Daten einer Organisation in ein Organization-Objekt um.
    Erwartet ein dict mit den Daten vom /orgs/{login}-Endpunkt.
    """
    org = Organization(
        org_id = api_data.get("id"),
        name = api_data.get("login"),
        website = api_data.get("blog"),
        email = api_data.get("email"),
        location = api_data.get("location"),
        created_at = datetime.datetime.strptime(api_data.get("created_at"), '%Y-%m-%dT%H:%M:%SZ') 
                     if api_data.get("created_at") else None
    )
    return org

def update_organizations():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole vollständige Repository-Daten von GitHub, um den Owner-Datensatz zu erhalten
            url = f"https://api.github.com/repositories/{repo_obj.repo_id}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Fehler beim Abrufen der Daten für Repository {repo_obj.name}: HTTP {response.status_code}")
                continue
            
            repo_data = response.json()
            owner_data = repo_data.get("owner")
            # Prüfe, ob der Owner eine Organisation ist
            if owner_data and owner_data.get("type") == "Organization":
                org_login = owner_data.get("login")
                # Rufe die vollständigen Details der Organisation ab
                org_details = get_organization_details(org_login)
                if not org_details:
                    print(f"Organisation {org_login} konnte nicht abgerufen werden.")
                    continue
                
                new_org = create_organization_from_api(org_details)
                # Prüfe, ob diese Organisation bereits in der DB existiert (über org_id)
                existing_org = session.query(Organization).filter_by(org_id=new_org.org_id).first()
                if existing_org:
                    # Update-Logik: Vergleiche und aktualisiere Felder, die sich ändern können
                    updated = False
                    if new_org.name != existing_org.name:
                        existing_org.name = new_org.name
                        updated = True
                    if new_org.website != existing_org.website:
                        existing_org.website = new_org.website
                        updated = True
                    if new_org.email != existing_org.email:
                        existing_org.email = new_org.email
                        updated = True
                    if new_org.location != existing_org.location:
                        existing_org.location = new_org.location
                        updated = True
                    # Wir aktualisieren created_at in der Regel nicht, da dies der ursprüngliche Erstellungszeitpunkt ist.
                    if updated:
                        print(f"Organisation {org_login} aktualisiert.")
                    else:
                        print(f"Organisation {org_login} ist aktuell. Überspringe.")
                else:
                    # Organisation existiert noch nicht – füge sie hinzu.
                    session.add(new_org)
                    print(f"Organisation {org_login} hinzugefügt.")
            else:
                print(f"Owner von Repository {repo_obj.name} ist keine Organisation. Überspringe.")
            time.sleep(1)  # Kurze Pause zur Schonung der API-Rate-Limits
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten von Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Organisationsdaten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_organizations()