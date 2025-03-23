"""Initial schema

Revision ID: 001
Create Date: 2025-03-22 18:12:05.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create organizations table
    op.create_table(
        'organizations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('login', sa.String(255), unique=True, nullable=False),
        sa.Column('name', sa.String(255)),
        sa.Column('description', sa.String(1024)),
        sa.Column('location', sa.String(255)),
        sa.Column('location_lat', sa.Float()),
        sa.Column('location_lon', sa.Float()),
        sa.Column('email', sa.String(255)),
        sa.Column('blog', sa.String(255)),
        sa.Column('twitter_username', sa.String(255)),
        sa.Column('is_verified', sa.Boolean(), default=False),
        sa.Column('has_organization_projects', sa.Boolean(), default=True),
        sa.Column('has_repository_projects', sa.Boolean(), default=True),
        sa.Column('public_repos', sa.Integer(), default=0),
        sa.Column('public_gists', sa.Integer(), default=0),
        sa.Column('followers', sa.Integer(), default=0),
        sa.Column('following', sa.Integer(), default=0),
        sa.Column('created_at', sa.DateTime()),
        sa.Column('updated_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create users table with location fields
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('login', sa.String(255), unique=True, nullable=False),
        sa.Column('name', sa.String(255)),
        sa.Column('email', sa.String(255)),
        sa.Column('location', sa.String(255)),
        sa.Column('location_lat', sa.Float()),
        sa.Column('location_lon', sa.Float()),
        sa.Column('blog', sa.String(255)),
        sa.Column('twitter_username', sa.String(255)),
        sa.Column('type', sa.String(50)),
        sa.Column('company', sa.String(255)),
        sa.Column('bio', sa.String(1024)),
        sa.Column('public_repos', sa.Integer(), default=0),
        sa.Column('public_gists', sa.Integer(), default=0),
        sa.Column('followers', sa.Integer(), default=0),
        sa.Column('following', sa.Integer(), default=0),
        sa.Column('created_at', sa.DateTime()),
        sa.Column('updated_at', sa.DateTime()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create organization_members table
    op.create_table(
        'organization_members',
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('role', sa.String(50)),
        sa.Column('joined_at', sa.DateTime()),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('organization_id', 'user_id')
    )
    
    # Create repositories table with organization_id
    op.create_table(
        'repositories',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('full_name', sa.String(511), unique=True, nullable=False),
        sa.Column('description', sa.String(1024)),
        sa.Column('language', sa.String(100)),
        sa.Column('homepage', sa.String(255)),
        sa.Column('default_branch', sa.String(255), default='main'),
        sa.Column('topics', sa.JSON),
        sa.Column('size', sa.Integer(), default=0),
        sa.Column('stars_count', sa.Integer(), default=0),
        sa.Column('forks_count', sa.Integer(), default=0),
        sa.Column('open_issues_count', sa.Integer(), default=0),
        sa.Column('watchers_count', sa.Integer(), default=0),
        sa.Column('private', sa.Boolean(), default=False),
        sa.Column('archived', sa.Boolean(), default=False),
        sa.Column('disabled', sa.Boolean(), default=False),
        sa.Column('fork', sa.Boolean(), default=False),
        sa.Column('allow_forking', sa.Boolean(), default=True),
        sa.Column('template', sa.Boolean(), default=False),
        sa.Column('created_at', sa.DateTime()),
        sa.Column('updated_at', sa.DateTime()),
        sa.Column('pushed_at', sa.DateTime()),
        sa.Column('owner_id', sa.Integer()),
        sa.Column('organization_id', sa.Integer()),
        sa.ForeignKeyConstraint(['owner_id'], ['users.id']),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id']),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create repository_contributors table
    op.create_table(
        'repository_contributors',
        sa.Column('repository_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('contributions', sa.Integer(), default=0),
        sa.Column('first_contribution_at', sa.DateTime()),
        sa.Column('last_contribution_at', sa.DateTime()),
        sa.ForeignKeyConstraint(['repository_id'], ['repositories.id']),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('repository_id', 'user_id')
    )
    
    # Create events table
    op.create_table(
        'events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.String(255), unique=True, nullable=False),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('payload', sa.JSON),
        sa.Column('public', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime()),
        sa.Column('repo_id', sa.Integer()),
        sa.Column('actor_id', sa.Integer()),
        sa.ForeignKeyConstraint(['repo_id'], ['repositories.id']),
        sa.ForeignKeyConstraint(['actor_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for efficient querying
    op.create_index('ix_organizations_login', 'organizations', ['login'])
    op.create_index('ix_users_login', 'users', ['login'])
    op.create_index('ix_repositories_full_name', 'repositories', ['full_name'])
    op.create_index('ix_repositories_owner_id', 'repositories', ['owner_id'])
    op.create_index('ix_repositories_organization_id', 'repositories', ['organization_id'])
    op.create_index('ix_events_type', 'events', ['type'])
    op.create_index('ix_events_created_at', 'events', ['created_at'])
    op.create_index('ix_events_repo_id', 'events', ['repo_id'])
    op.create_index('ix_events_actor_id', 'events', ['actor_id'])

def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_events_actor_id')
    op.drop_index('ix_events_repo_id')
    op.drop_index('ix_events_created_at')
    op.drop_index('ix_events_type')
    op.drop_index('ix_repositories_organization_id')
    op.drop_index('ix_repositories_owner_id')
    op.drop_index('ix_repositories_full_name')
    op.drop_index('ix_users_login')
    op.drop_index('ix_organizations_login')
    
    # Drop tables
    op.drop_table('events')
    op.drop_table('repository_contributors')
    op.drop_table('repositories')
    op.drop_table('organization_members')
    op.drop_table('users')
    op.drop_table('organizations')
