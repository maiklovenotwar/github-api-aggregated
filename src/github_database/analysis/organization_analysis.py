"""Module for analyzing organization-related activities and metrics."""

from sqlalchemy import func, desc
from sqlalchemy.orm import Session
from ..database.database import Organization, Repository, User, Event
from typing import List, Dict, Any
import pandas as pd


def get_organization_growth(session: Session, org_id: int, days: int = 365) -> pd.DataFrame:
    """Analyze organization growth over time (members, repositories, events)."""
    # Member growth
    member_query = (
        session.query(
            func.date(Event.created_at).label('date'),
            func.count(func.distinct(Event.actor_id)).label('new_members')
        )
        .join(Repository)
        .filter(Repository.organization_id == org_id)
        .filter(Event.created_at >= func.datetime('now', f'-{days} days'))
        .group_by(func.date(Event.created_at))
        .order_by('date')
    )
    
    # Repository growth
    repo_query = (
        session.query(
            func.date(Repository.created_at).label('date'),
            func.count(Repository.id).label('new_repositories')
        )
        .filter(Repository.organization_id == org_id)
        .filter(Repository.created_at >= func.datetime('now', f'-{days} days'))
        .group_by(func.date(Repository.created_at))
        .order_by('date')
    )
    
    # Combine data
    members_df = pd.DataFrame(member_query.all())
    repos_df = pd.DataFrame(repo_query.all())
    
    return pd.merge(members_df, repos_df, on='date', how='outer').fillna(0)


def get_top_contributors(session: Session, org_id: int, limit: int = 10) -> pd.DataFrame:
    """Get top contributors across all repositories in an organization."""
    query = (
        session.query(
            User.login,
            User.location,
            func.count(Event.id).label('contribution_count')
        )
        .join(Event, Event.actor_id == User.id)
        .join(Repository, Event.repo_id == Repository.id)
        .filter(Repository.organization_id == org_id)
        .filter(Event.type.in_(['PushEvent', 'PullRequestEvent']))
        .group_by(User.login, User.location)
        .order_by(desc('contribution_count'))
        .limit(limit)
    )
    return pd.DataFrame(query.all())


def get_repository_activity(session: Session, org_id: int) -> pd.DataFrame:
    """Analyze repository activity within an organization."""
    query = (
        session.query(
            Repository.name,
            Repository.language,
            Repository.stars_count,
            Repository.forks_count,
            func.count(Event.id).label('event_count')
        )
        .outerjoin(Event)
        .filter(Repository.organization_id == org_id)
        .group_by(
            Repository.name,
            Repository.language,
            Repository.stars_count,
            Repository.forks_count
        )
        .order_by(desc('event_count'))
    )
    return pd.DataFrame(query.all())
