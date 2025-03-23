"""Module for analyzing location-based repository and user data."""

from sqlalchemy import func, desc
from sqlalchemy.orm import Session
from ..database.database import User, Repository, Organization, Event
from typing import List, Dict, Any
import pandas as pd


def get_contributor_locations(session: Session, repository_id: int) -> pd.DataFrame:
    """Get geographical distribution of contributors for a repository."""
    query = (
        session.query(
            User.location,
            User.location_lat,
            User.location_lon,
            func.count(User.id).label('contributor_count')
        )
        .join(Event, Event.actor_id == User.id)
        .filter(Event.repo_id == repository_id)
        .filter(User.location.isnot(None))
        .group_by(User.location, User.location_lat, User.location_lon)
        .order_by(desc('contributor_count'))
    )
    return pd.DataFrame(query.all())


def get_organization_locations(session: Session) -> pd.DataFrame:
    """Get geographical distribution of organizations."""
    query = (
        session.query(
            Organization.location,
            Organization.location_lat,
            Organization.location_lon,
            func.count(Repository.id).label('repository_count'),
            func.sum(Repository.stars_count).label('total_stars')
        )
        .outerjoin(Repository)
        .filter(Organization.location.isnot(None))
        .group_by(Organization.location, Organization.location_lat, Organization.location_lon)
        .order_by(desc('total_stars'))
    )
    return pd.DataFrame(query.all())


def get_active_regions(session: Session, event_type: str = None, days: int = 30) -> pd.DataFrame:
    """Get most active regions based on event activity."""
    query = (
        session.query(
            User.location,
            User.location_lat,
            User.location_lon,
            func.count(Event.id).label('event_count')
        )
        .join(Event, Event.actor_id == User.id)
        .filter(User.location.isnot(None))
        .filter(Event.created_at >= func.datetime('now', f'-{days} days'))
    )
    
    if event_type:
        query = query.filter(Event.type == event_type)
    
    query = (
        query.group_by(User.location, User.location_lat, User.location_lon)
        .order_by(desc('event_count'))
    )
    
    return pd.DataFrame(query.all())
