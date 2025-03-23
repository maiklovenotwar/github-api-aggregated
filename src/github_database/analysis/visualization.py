"""Module for visualizing analysis results."""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, Any


def plot_location_heatmap(df: pd.DataFrame, 
                         lat_col: str = 'location_lat',
                         lon_col: str = 'location_lon',
                         size_col: str = None,
                         title: str = 'Geographic Distribution') -> go.Figure:
    """Create a heatmap of locations on a world map."""
    fig = px.density_mapbox(
        df,
        lat=lat_col,
        lon=lon_col,
        z=size_col if size_col else None,
        radius=30,
        center=dict(lat=0, lon=0),
        zoom=1,
        mapbox_style="carto-positron",
        title=title
    )
    return fig


def plot_organization_growth(growth_df: pd.DataFrame) -> go.Figure:
    """Create a subplot showing organization growth metrics over time."""
    fig = make_subplots(rows=2, cols=1,
                       subplot_titles=('Member Growth', 'Repository Growth'))
    
    # Member growth line
    fig.add_trace(
        go.Scatter(x=growth_df['date'], 
                  y=growth_df['new_members'].cumsum(),
                  name='Total Members'),
        row=1, col=1
    )
    
    # Repository growth line
    fig.add_trace(
        go.Scatter(x=growth_df['date'], 
                  y=growth_df['new_repositories'].cumsum(),
                  name='Total Repositories'),
        row=2, col=1
    )
    
    fig.update_layout(height=800, title_text="Organization Growth Over Time")
    return fig


def plot_language_distribution(df: pd.DataFrame,
                             value_col: str = 'repository_count',
                             title: str = 'Language Distribution') -> go.Figure:
    """Create a pie chart showing language distribution."""
    fig = px.pie(df,
                 values=value_col,
                 names='language',
                 title=title)
    return fig


def plot_activity_timeline(df: pd.DataFrame,
                          date_col: str = 'date',
                          value_col: str = 'event_count',
                          title: str = 'Activity Timeline') -> go.Figure:
    """Create a timeline of activity."""
    fig = px.line(df,
                  x=date_col,
                  y=value_col,
                  title=title)
    fig.update_traces(mode='lines+markers')
    return fig
